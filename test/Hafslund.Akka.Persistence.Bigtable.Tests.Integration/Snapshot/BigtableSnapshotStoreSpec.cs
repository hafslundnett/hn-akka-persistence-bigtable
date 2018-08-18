using Akka.Configuration;
using Akka.Persistence.TCK.Snapshot;

namespace Hafslund.Akka.Persistence.Bigtable.Tests.Integration.Snapshot
{
    public class BigtableSnapshotStoreSpec : SnapshotStoreSpec
    {
        private static string _tableName = "projects/hafslund-mdm-test/instances/mdm-meterreadings-test/tables/IntegrationTestSnapshotStore";

        private static readonly Config SpecConfig =
            ConfigurationFactory.ParseString(@"

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
                                table-name = """ + _tableName + @"""
                                auto-initialize = on
                            }
                        }
                    }
                }");

        public BigtableSnapshotStoreSpec() : base(SpecConfig)
        {
            Initialize();
        }
    }
}