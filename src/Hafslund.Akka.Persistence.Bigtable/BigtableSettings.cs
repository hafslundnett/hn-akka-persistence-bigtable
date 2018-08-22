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

        /// <summary>
        /// The name of the column familiy
        /// </summary>
        /// <example>f</example>
        public string FamilyName { get; }

        protected BigtableSettings(string tableName, string familyName)
        {
            TableName = tableName;
            FamilyName = familyName;
        }

        protected BigtableSettings(Config config) : this(config.GetString("table-name"), config.GetString("family-name"))
        {
        }
    }
}
