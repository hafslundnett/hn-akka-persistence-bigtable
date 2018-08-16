namespace Hafslund.Akka.Persistence.Bigtable.Journal
{
    public class ShardingBigtableJournal : BigtableJournal
    {
        //protected override string GetTableName()
        //{
        //    return Context.System.Settings.Config.GetConfig("akka.persistence.journal.Bigtable-sharding").GetString("table-name");
        //}
    }
}