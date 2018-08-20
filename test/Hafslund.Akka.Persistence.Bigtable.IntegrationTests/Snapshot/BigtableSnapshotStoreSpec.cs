using System;
using Akka.Configuration;
using Akka.Persistence.TCK.Snapshot;
using Google.Cloud.Bigtable.Common.V2;
using Google.Cloud.Bigtable.V2;
using Hafslund.Akka.Persistence.Bigtable.IntegrationTests;

namespace Hafslund.Akka.Persistence.Bigtable.Tests.Integration.Snapshot
{
    public class BigtableSnapshotStoreSpec : SnapshotStoreSpec
    {
        private readonly static string TableName;
        private static readonly Config SpecConfig;

        static BigtableSnapshotStoreSpec()
        {
            var timefactorString = Environment.GetEnvironmentVariable("INTEGRATION_TEST_TIME_FACTOR");
            var timeFactor = timefactorString == null ? 1 : int.Parse(timefactorString);
            Console.WriteLine($"BigtableSnapshotStoreSpec timefactor: {timeFactor}");

            TableName = Environment.GetEnvironmentVariable("INTEGRATION_TEST_SNAPSHOT_STORE_TABLE");
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