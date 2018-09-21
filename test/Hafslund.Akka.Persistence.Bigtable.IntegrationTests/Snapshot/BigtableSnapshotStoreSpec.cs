using System;
using System.Collections.Generic;
using Akka.Actor;
using Akka.Configuration;
using Akka.Persistence;
using Akka.Persistence.TCK.Snapshot;
using Google.Cloud.Bigtable.Common.V2;
using Google.Cloud.Bigtable.V2;
using Hafslund.Akka.Persistence.Bigtable.IntegrationTests;
using Microsoft.Extensions.Configuration;
using Xunit;

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

            var timeFactor = int.Parse(config.GetValue("INTEGRATION_TEST_TIME_FACTOR", "1"));
            Console.WriteLine($"BigtableSnapshotStoreSpec timefactor: {timeFactor}");

            TableName = config.GetValue("INTEGRATION_TEST_SNAPSHOT_STORE_TABLE", "NOT_SET");
            Console.WriteLine($"BigtableSnapshotStoreSpec bigtable table: {TableName}");

            SpecConfig = ConfigurationFactory.ParseString($"akka.test.timefactor={timeFactor}")
                .WithFallback(ConfigurationFactory.ParseString(@"
                akka {
                    actor {
                        serialize-messages = on
                        serializers {
                            my-event-serializer = ""Hafslund.Akka.Persistence.Bigtable.IntegrationTests.MyEventSerializer, Hafslund.Akka.Persistence.Bigtable.IntegrationTests""
                        }
                        serialization-bindings {
                            ""Hafslund.Akka.Persistence.Bigtable.IntegrationTests.MyEvent, Hafslund.Akka.Persistence.Bigtable.IntegrationTests"" = my-event-serializer
                        }
                        serialization-identifiers {
                            ""Hafslund.Akka.Persistence.Bigtable.IntegrationTests.MyEventSerializer, Hafslund.Akka.Persistence.Bigtable.IntegrationTests"" = 9999
                        }
                    }
                    persistence {
                        transport-serialization {
                            bigtable {
                                hostname = ""localhost""
                                transport-protocol = akka.tcp
                                port = 2552
                            }
                        }
                        publish-plugin-commands = on
                        snapshot-store {
                            plugin = ""akka.persistence.snapshot-store.bigtable""
                            bigtable {
                                enable-serialization-with-transport = true
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

        private void ClearTable()
        {
            var rowRange = RowRange.Closed(new BigtableByteString($"{Pid}"), new BigtableByteString($"{Pid}~"));
            BigtableTestUtils.DeleteRows(TableName, rowRange);
        }

        [Fact]
        public void SnapshotStore_should_serialize_with_transport_if_enabled()
        {
            //Given
            var ser = Sys.Serialization.FindSerializerForType(typeof(ActorRefWrapper));
            var receiver = CreateTestProbe();
            var myEvent = new ActorRefWrapper
            {
                ActorRef = CreateTestActor("test-actor")
            };

            //When
            var metadata = new SnapshotMetadata(Pid, 0);
            SnapshotStore.Tell(new SaveSnapshot(metadata, myEvent), receiver.Ref);
            receiver.ExpectMsg<SaveSnapshotSuccess>();
            SnapshotStore.Tell(new LoadSnapshot(Pid, SnapshotSelectionCriteria.Latest, 1), receiver.Ref);

            //Then
            receiver.ExpectMsg<LoadSnapshotResult>(msg =>
            {
                var e = (ActorRefWrapper)msg.Snapshot.Snapshot;
                Assert.True(e.IsSerializedWithTransport, "snapshot should be serialized with transport");
            });
        }
    }
}