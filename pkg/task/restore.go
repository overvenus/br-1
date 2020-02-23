package task

import (
	"context"
	"math"

	"github.com/pingcap/errors"
	"github.com/pingcap/kvproto/pkg/backup"
	"github.com/pingcap/log"
	"github.com/pingcap/tidb-tools/pkg/filter"
	"github.com/spf13/pflag"
	"go.uber.org/zap"

	"github.com/pingcap/br/pkg/conn"
	"github.com/pingcap/br/pkg/restore"
	"github.com/pingcap/br/pkg/summary"
	"github.com/pingcap/br/pkg/utils"
	"github.com/pingcap/br/pkg/utils/rtree"
)

const (
	flagOnline = "online"
)

var schedulers = map[string]struct{}{
	"balance-leader-scheduler":     {},
	"balance-hot-region-scheduler": {},
	"balance-region-scheduler":     {},

	"shuffle-leader-scheduler":     {},
	"shuffle-region-scheduler":     {},
	"shuffle-hot-region-scheduler": {},
}

var pdRegionMergeCfg = []string{
	"max-merge-region-keys",
	"max-merge-region-size",
}

var pdScheduleLimitCfg = []string{
	"leader-schedule-limit",
	"region-schedule-limit",
	"max-snapshot-count",
}

// RestoreConfig is the configuration specific for restore tasks.
type RestoreConfig struct {
	Config

	Online bool `json:"online" toml:"online"`
}

// DefineRestoreFlags defines common flags for the restore command.
func DefineRestoreFlags(flags *pflag.FlagSet) {
	flags.Bool("online", false, "Whether online when restore")
	// TODO remove hidden flag if it's stable
	_ = flags.MarkHidden("online")
}

// ParseFromFlags parses the restore-related flags from the flag set.
func (cfg *RestoreConfig) ParseFromFlags(flags *pflag.FlagSet) error {
	var err error
	cfg.Online, err = flags.GetBool(flagOnline)
	if err != nil {
		return errors.Trace(err)
	}
	return cfg.Config.ParseFromFlags(flags)
}

// RunRestore starts a restore task inside the current goroutine.
func RunRestore(c context.Context, cmdName string, cfg *RestoreConfig) error {
	ctx, cancel := context.WithCancel(c)
	defer cancel()

	mgr, err := newMgr(ctx, cfg.PD)
	if err != nil {
		return err
	}
	defer mgr.Close()

	client, err := restore.NewRestoreClient(ctx, mgr.GetPDClient(), mgr.GetTiKV())
	if err != nil {
		return err
	}
	defer client.Close()

	client.SetRateLimit(cfg.RateLimit)
	client.SetConcurrency(uint(cfg.Concurrency))
	if cfg.Online {
		client.EnableOnline()
	}

	defer summary.Summary(cmdName)

	u, _, backupMeta, err := ReadBackupMeta(ctx, &cfg.Config)
	if err != nil {
		return err
	}
	if err = client.InitBackupMeta(backupMeta, u); err != nil {
		return err
	}

	files, tables, err := filterRestoreFiles(client, cfg)
	if err != nil {
		return err
	}
	if len(files) == 0 {
		return errors.New("all files are filtered out from the backup archive, nothing to restore")
	}
	summary.CollectInt("restore files", len(files))

	var newTS uint64
	if client.IsIncremental() {
		newTS, err = client.GetTS(ctx)
		if err != nil {
			return err
		}
	}
	rewriteRules, newTables, err := client.CreateTables(mgr.GetDomain(), tables, newTS)
	if err != nil {
		return err
	}

	ranges, err := restore.ValidateFileRanges(files, rewriteRules)
	if err != nil {
		return err
	}
	summary.CollectInt("restore ranges", len(ranges))

	ranges = restore.AttachFilesToRanges(files, ranges)

	// Redirect to log if there is no log file to avoid unreadable output.
	updateCh := utils.StartProgress(
		ctx,
		cmdName,
		// Split/Scatter + Download/Ingest
		int64(len(ranges)+len(files)),
		!cfg.LogProgress)

	clusterCfg, err := restorePreWork(ctx, client, mgr)
	if err != nil {
		return err
	}

	// Do not reset timestamp if we are doing incremental restore, because
	// we are not allowed to decrease timestamp.
	if !client.IsIncremental() {
		if err = client.ResetTS(cfg.PD); err != nil {
			log.Error("reset pd TS failed", zap.Error(err))
			return err
		}
	}

	// Restore sst files in batch, the max size of batches are 64.
	batchSize := 128
	batches := make([][]rtree.Range, 0, (len(ranges)+batchSize-1)/batchSize)
	for batchSize < len(ranges) {
		ranges, batches = ranges[batchSize:], append(batches, ranges[0:batchSize:batchSize])
	}
	batches = append(batches, ranges)

	for _, rangeBatch := range batches {
		// Split regions by the given rangeBatch.
		err = restore.SplitRanges(ctx, client, rangeBatch, rewriteRules, updateCh)
		if err != nil {
			log.Error("split regions failed", zap.Error(err))
			return err
		}

		// Collect related files in the given rangeBatch.
		fileBatch := make([]*backup.File, 0, 2*len(rangeBatch))
		for _, rg := range rangeBatch {
			fileBatch = append(fileBatch, rg.Files...)
		}

		// After split, we can restore backup files.
		err = client.RestoreFiles(fileBatch, rewriteRules, updateCh)
		if err != nil {
			break
		}
	}

	// Always run the post-work even on error, so we don't stuck in the import
	// mode or emptied schedulers
	restorePostWork(ctx, client, mgr, clusterCfg)
	if err != nil {
		return err
	}

	// Restore has finished.
	close(updateCh)

	// Checksum
	updateCh = utils.StartProgress(
		ctx, "Checksum", int64(len(newTables)), !cfg.LogProgress)
	err = client.ValidateChecksum(
		ctx, mgr.GetTiKV().GetClient(), tables, newTables, updateCh)
	if err != nil {
		return err
	}
	close(updateCh)

	return nil
}

func filterRestoreFiles(
	client *restore.Client,
	cfg *RestoreConfig,
) (files []*backup.File, tables []*utils.Table, err error) {
	tableFilter, err := filter.New(cfg.CaseSensitive, &cfg.Filter)
	if err != nil {
		return nil, nil, err
	}

	for _, db := range client.GetDatabases() {
		createdDatabase := false
		for _, table := range db.Tables {
			if !tableFilter.Match(&filter.Table{Schema: db.Schema.Name.O, Name: table.Schema.Name.O}) {
				continue
			}

			if !createdDatabase {
				if err = client.CreateDatabase(db.Schema); err != nil {
					return nil, nil, err
				}
				createdDatabase = true
			}

			files = append(files, table.Files...)
			tables = append(tables, table)
		}
	}

	return
}

type clusterConfig struct {
	// Enable PD schedulers before restore
	scheduler []string
	// Region merge configuration before restore
	mergeCfg map[string]int
	// Scheudle limits configuration before restore
	scheduleLimitCfg map[string]int
}

// restorePreWork executes some prepare work before restore
func restorePreWork(ctx context.Context, client *restore.Client, mgr *conn.Mgr) (clusterConfig, error) {
	if client.IsOnline() {
		return clusterConfig{}, nil
	}

	// Switch TiKV cluster to import mode (adjust rocksdb configuration).
	if err := client.SwitchToImportMode(ctx); err != nil {
		return clusterConfig{}, nil
	}

	// Remove default PD scheduler that may affect restore process.
	existSchedulers, err := mgr.ListSchedulers(ctx)
	if err != nil {
		return clusterConfig{}, nil
	}
	needRemoveSchedulers := make([]string, 0, len(existSchedulers))
	for _, s := range existSchedulers {
		if _, ok := schedulers[s]; ok {
			needRemoveSchedulers = append(needRemoveSchedulers, s)
		}
	}
	scheduler, err := removePDLeaderScheduler(ctx, mgr, needRemoveSchedulers)
	if err != nil {
		return clusterConfig{}, nil
	}

	stores, err := mgr.GetPDClient().GetAllStores(ctx)
	if err != nil {
		return clusterConfig{}, err
	}

	mergeCfg := make(map[string]int)
	for _, cfgKey := range pdRegionMergeCfg {
		value, err := mgr.GetPDScheduleConfig(ctx, cfgKey)
		if err != nil {
			return clusterConfig{}, err
		}
		if value == nil {
			// Ignore non-exist config.
			continue
		}
		mergeCfg[cfgKey] = int(value.(float64))

		// Disable region merge by setting config to 0.
		err = mgr.UpdatePDScheduleConfig(ctx, cfgKey, 0)
		if err != nil {
			return clusterConfig{}, err
		}
	}

	scheduleLimitCfg := make(map[string]int)
	for _, cfgKey := range pdScheduleLimitCfg {
		value, err := mgr.GetPDScheduleConfig(ctx, cfgKey)
		if err != nil {
			return clusterConfig{}, err
		}
		if value == nil {
			// Ignore non-exist config.
			continue
		}
		limit := int(value.(float64))
		scheduleLimitCfg[cfgKey] = limit

		// Speed update PD scheduler by enlarging scheduling limits.
		// Multiply limits by store count but no more than 40.
		// TODO: why 40?
		newLimits := math.Max(40, float64(limit*len(stores)))
		err = mgr.UpdatePDScheduleConfig(ctx, cfgKey, int(newLimits))
		if err != nil {
			return clusterConfig{}, err
		}
	}

	cluster := clusterConfig{
		scheduler:        scheduler,
		mergeCfg:         mergeCfg,
		scheduleLimitCfg: scheduleLimitCfg,
	}
	return cluster, nil
}

func removePDLeaderScheduler(ctx context.Context, mgr *conn.Mgr, existSchedulers []string) ([]string, error) {
	removedSchedulers := make([]string, 0, len(existSchedulers))
	for _, scheduler := range existSchedulers {
		err := mgr.RemoveScheduler(ctx, scheduler)
		if err != nil {
			return nil, err
		}
		removedSchedulers = append(removedSchedulers, scheduler)
	}
	return removedSchedulers, nil
}

// restorePostWork executes some post work after restore
func restorePostWork(
	ctx context.Context, client *restore.Client, mgr *conn.Mgr, clusterCfg clusterConfig,
) {
	if client.IsOnline() {
		return
	}
	if err := client.SwitchToNormalMode(ctx); err != nil {
		log.Warn("fail to switch to normal mode")
	}
	if err := addPDLeaderScheduler(ctx, mgr, clusterCfg.scheduler); err != nil {
		log.Warn("fail to add PD schedulers")
	}
	for cfgKey, cfgValue := range clusterCfg.mergeCfg {
		if err := mgr.UpdatePDScheduleConfig(ctx, cfgKey, cfgValue); err != nil {
			log.Warn("fail to update PD region merge config")
		}
	}
	for cfgKey, cfgValue := range clusterCfg.scheduleLimitCfg {
		if err := mgr.UpdatePDScheduleConfig(ctx, cfgKey, cfgValue); err != nil {
			log.Warn("fail to update PD scheule limit config")
		}
	}
}

func addPDLeaderScheduler(ctx context.Context, mgr *conn.Mgr, removedSchedulers []string) error {
	for _, scheduler := range removedSchedulers {
		err := mgr.AddScheduler(ctx, scheduler)
		if err != nil {
			return err
		}
	}
	return nil
}
