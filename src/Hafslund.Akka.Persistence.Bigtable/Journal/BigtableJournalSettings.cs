using Akka.Configuration;

namespace Hafslund.Akka.Persistence.Bigtable.Journal
{
    public class BigtableJournalSettings : BigtableSettings
    {
        protected BigtableJournalSettings(string tableName, string familyName) : base(tableName, familyName)
        {
        }

        public new static BigtableJournalSettings Create(Config config)
        {
            return (BigtableJournalSettings)BigtableSettings.Create(config);
        }
    }
}
