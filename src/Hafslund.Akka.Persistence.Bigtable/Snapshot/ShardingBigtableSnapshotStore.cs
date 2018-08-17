namespace Hafslund.Akka.Persistence.Bigtable.Snapshot
{
    public class ShardingBigtableSnapshotStore : BigtableSnapshotStore
    {
        protected override string GetTableName()
        {
            return Context.System.Settings.Config.GetConfig("akka.persistence.snapshot-store.Bigtable-sharding").GetString("table-name");
        }
    }
}