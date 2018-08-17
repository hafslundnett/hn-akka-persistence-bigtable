using Akka.Configuration;

namespace Hafslund.Akka.Persistence.Bigtable
{
    /// <summary>
    ///     Defines all of the configuration settings used by the `akka.persistence.journal.bigtable` plugin.
    /// </summary>
    public class BigtableSettings
    {
        /// <summary>
        /// The name of the table for normal actor data
        /// </summary>
        /// <example>projects/[project-id]/instances/[instance-id]/tables/[table-name]</example>
        public string TableName { get; }

        protected BigtableSettings(string tableName)
        {
            TableName = tableName;
        }

        public static BigtableSettings Create(Config config)
        {
            var tableName = config.GetString("table-name");
            return new BigtableSettings(tableName);
        }
    }
}
