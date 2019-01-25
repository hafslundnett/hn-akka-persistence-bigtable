using System;
using Akka.Configuration;
using Akka.Persistence;
using Akka.Persistence.TCK.Snapshot;
using Microsoft.Extensions.Configuration;
using Xunit;

namespace Hafslund.Akka.Persistence.Bigtable.IntegrationTests.Snapshot
{
    /// <summary>
    /// To run tests locally, first run: gcloud beta emulators bigtable start --host-port localhost:8090
    /// </summary>
    public class BigtableSnapshotStoreSpec : SnapshotStoreSpec
    {
        public static readonly string Host = GetEnvOrDefault("BIGTABLE_EMULATOR_HOST", "localhost:8090");

        private const string ProjectId = "my-project";
        private const string InstanceId = "my-instance";

        private static readonly string TableName = "SnapshotStoreSpec";
        private static readonly Config SpecConfig;

        public static IConfigurationRoot ReadConfig()
        {
            return new ConfigurationBuilder()
                .AddJsonFile("appsettings.Development.json", optional: true)
                .AddEnvironmentVariables()
                .Build();
        }

        private static string GetTablePath()
        {
            return $"projects/{ProjectId}/instances/{InstanceId}/tables/{TableName}";
        }

        static BigtableSnapshotStoreSpec()
        {
            Console.WriteLine($"BigtableSnapshotStoreSpec bigtable table: {TableName}");

            SpecConfig = ConfigurationFactory.ParseString($"akka.test.timefactor=10")
                .WithFallback(ConfigurationFactory.ParseString(@"
                akka {
                    actor {
                        serializers {
                            messagepack = ""Akka.Serialization.MessagePack.MsgPackSerializer, Akka.Serialization.MessagePack""
                            actor-ref-wrapper-serializer = ""Hafslund.Akka.Persistence.Bigtable.IntegrationTests.ActorRefWrapperSerializer, Hafslund.Akka.Persistence.Bigtable.IntegrationTests""
                        }
                        serialization-bindings {
                            ""System.Object"" = messagepack
                            ""Hafslund.Akka.Persistence.Bigtable.IntegrationTests.ActorRefWrapper, Hafslund.Akka.Persistence.Bigtable.IntegrationTests"" = actor-ref-wrapper-serializer
                        }
                        serialization-identifiers {
                            ""Hafslund.Akka.Persistence.Bigtable.IntegrationTests.ActorRefWrapperSerializer, Hafslund.Akka.Persistence.Bigtable.IntegrationTests"" = 9999
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
                                class = ""Hafslund.Akka.Persistence.Bigtable.IntegrationTests.Snapshot.BigtableSnapshotStoreTester, Hafslund.Akka.Persistence.Bigtable.IntegrationTests""
                                plugin-dispatcher = ""akka.actor.default-dispatcher""
                                table-name = """ + GetTablePath() + @"""
                                auto-initialize = on
                            }
                        }
                    }
                }"));
        }
        private static string GetEnvOrDefault(string name, string defaultValue)
        {
            return Environment.GetEnvironmentVariable(name) ?? defaultValue;
        }

        public BigtableSnapshotStoreSpec() : base(SpecConfig)
        {
            BigtableTestUtils.InitializeWithEmulator(Host, ProjectId, InstanceId, TableName);
            Initialize();
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