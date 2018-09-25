using Akka.Configuration;

namespace Hafslund.Akka.Persistence.Bigtable.Journal
{
    public class BigtableJournalSettings : BigtableSettings
    {
        protected BigtableJournalSettings(Config config) : base(config)
        {
        }

        public static BigtableJournalSettings Create(Config config)
        {
            return new BigtableJournalSettings(config);
        }
    }
}
