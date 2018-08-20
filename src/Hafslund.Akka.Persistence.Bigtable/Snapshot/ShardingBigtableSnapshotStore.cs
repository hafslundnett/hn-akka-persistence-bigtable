namespace Hafslund.Akka.Persistence.Bigtable.Snapshot
{
    public class ShardingBigtableSnapshotStore : BigtableSnapshotStore
    {
        public ShardingBigtableSnapshotStore() : base(ShardingBigtablePersistence.Get(Context.System).BigtableSnapshotSettings)
        {
        }
    }
}
