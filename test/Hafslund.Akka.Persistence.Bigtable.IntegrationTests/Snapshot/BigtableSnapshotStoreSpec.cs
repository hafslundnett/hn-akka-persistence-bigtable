using System;
using Akka.Configuration;
using Akka.Persistence.TCK.Snapshot;
using Google.Cloud.Bigtable.Common.V2;
using Google.Cloud.Bigtable.V2;
using Hafslund.Akka.Persistence.Bigtable.IntegrationTests;
using Microsoft.Extensions.Configuration;

namespace Hafslund.Akka.Persistence.Bigtable.Tests.Integration.Snapshot
{
    public class BigtableSnapshotStoreSpec : SnapshotStoreSpec
    {
        private readonly static string TableName;
        private static readonly Config SpecConfig;
        
        public static IConfigurationRoot ReadConfig()
        {
            return new ConfigurationBuilder()
                .AddJsonFile("appsettings.Development.json", optional: true)
                .AddEnvironmentVariables()
                .Build();
        }

        static BigtableSnapshotStoreSpec()
        {
            var config = ReadConfig();

            var timeFactor= int.Parse(config.GetValue("INTEGRATION_TEST_TIME_FACTOR", "1"));
            Console.WriteLine($"BigtableSnapshotStoreSpec timefactor: {timeFactor}");

            TableName = config.GetValue("INTEGRATION_TEST_SNAPSHOT_STORE_TABLE", "NOT_SET");
            Console.WriteLine($"BigtableSnapshotStoreSpec bigtable table: {TableName}");

            SpecConfig = ConfigurationFactory.ParseString($"akka.test.timefactor={timeFactor}")
                .WithFallback(ConfigurationFactory.ParseString(@"
                akka {
                    serializers {
                        messagepack = ""Akka.Serialization.MessagePack.MsgPackSerializer, Akka.Serialization.MessagePack""
                    }
                    persistence {
                        publish-plugin-commands = on
                        snapshot-store {
                            plugin = ""akka.persistence.snapshot-store.bigtable""
                            bigtable {
                                class = ""Hafslund.Akka.Persistence.Bigtable.Snapshot.BigtableSnapshotStore, Hafslund.Akka.Persistence.Bigtable""
                                plugin-dispatcher = ""akka.actor.default-dispatcher""
                                table-name = """ + TableName + @"""
                                auto-initialize = on
                            }
                        }
                    }
                }"));
        }

        public BigtableSnapshotStoreSpec() : base(SpecConfig)
        {
            ClearTable();
            Initialize();
        }

        public void ClearTable()
        {
            var rowRange = RowRange.Closed(new BigtableByteString("p-"), new BigtableByteString("p-~"));
            BigtableTestUtils.DeleteRows(TableName, rowRange);
        }
    }
}