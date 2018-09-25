namespace Hafslund.Akka.Persistence.Bigtable.Journal
{
    public class ShardingBigtableJournal : BigtableJournal
    {
        public ShardingBigtableJournal() : base(ShardingBigtablePersistence.Get(Context.System).BigtableJournalSettings)
        {
        }
    }
}
