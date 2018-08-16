using Akka.Configuration;
using System;
using System.Collections.Generic;
using System.Text;

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
        /// <example>projects/hafslund-mdm-test/instances/mdm-meterreadings-test/tables/ActorEvents</example>
        public string TableName { get; }

        /// <summary>
        /// The name of the table for sharding data. If left empty or unconfigured, the same as TableName will be used.
        /// </summary>
        /// <example>projects/hafslund-mdm-test/instances/mdm-meterreadings-test/tables/ShardingEvents</example>
        public string ShardingTableName { get; }

        protected BigtableSettings(string tableName, string shardingTableName)
        {
            TableName = tableName;
            ShardingTableName = shardingTableName;
        }

        public static BigtableSettings Create(Config config)
        {
            var tableName = config.GetString("table-name");

            var shardingTableName = config.GetString("sharding-table-name");
            shardingTableName = string.IsNullOrWhiteSpace(shardingTableName) ? tableName : shardingTableName;

            return new BigtableSettings(tableName, shardingTableName);
        }
    }
}
