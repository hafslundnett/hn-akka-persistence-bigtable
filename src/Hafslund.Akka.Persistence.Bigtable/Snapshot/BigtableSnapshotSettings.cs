using Akka.Configuration;

namespace Hafslund.Akka.Persistence.Bigtable.Snapshot
{
    public class BigtableSnapshotSettings : BigtableSettings
    {
        protected BigtableSnapshotSettings(Config config) : base(config)
        {
        }

        public static BigtableSnapshotSettings Create(Config config)
        {
            return new BigtableSnapshotSettings(config);
        }
    }
}
