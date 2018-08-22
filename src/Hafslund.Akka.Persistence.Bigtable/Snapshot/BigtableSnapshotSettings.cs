using Akka.Configuration;

namespace Hafslund.Akka.Persistence.Bigtable.Snapshot
{
    public class BigtableSnapshotSettings : BigtableSettings
    {
        protected BigtableSnapshotSettings(string tableName, string familyName) : base(tableName, familyName)
        {
        }

        public new static BigtableSnapshotSettings Create(Config config)
        {
            return (BigtableSnapshotSettings)BigtableSettings.Create(config);
        }
    }
}
