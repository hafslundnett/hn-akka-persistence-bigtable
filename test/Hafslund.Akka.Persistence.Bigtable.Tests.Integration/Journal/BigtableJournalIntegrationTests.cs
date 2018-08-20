using System.Collections.Generic;
using System.Linq;
using Akka.Persistence.TCK.Journal;
using Google.Cloud.Bigtable.Common.V2;
using Google.Cloud.Bigtable.V2;
using Akka.Configuration;

namespace Hafslund.Akka.Persistence.Bigtable.Tests.Integration.Journal
{
    public class BigtableJournalIntegrationTests : JournalSpec
    {
        private static string _tableName = "projects/hafslund-mdm-test/instances/mdm-meterreadings-test/tables/IntegrationTestActorEvents";

        private static readonly Config SpecConfig =
            ConfigurationFactory.ParseString(@"

                akka {
                    serializers {
                        messagepack = ""Akka.Serialization.MessagePack.MsgPackSerializer, Akka.Serialization.MessagePack""
                    }
                    persistence {
                        auto-start-journals = [""akka.persistence.journal.bigtable""]
                        publish-plugin-commands = on
                        journal {
                            plugin = ""akka.persistence.journal.bigtable""
                            bigtable {
                                class = ""Hafslund.Akka.Persistence.Bigtable.Journal.BigtableJournal, Hafslund.Akka.Persistence.Bigtable""
                                plugin-dispatcher = ""akka.actor.default-dispatcher""
                                table-name = """ + _tableName + @"""
                                auto-initialize = on
                                default-serializer = messagepack
                            }
                        }
                    }
                }");

        public BigtableJournalIntegrationTests() : base(SpecConfig)
        {
            Initialize();
        }

        protected override void PreparePersistenceId(string pid)
        {
            var bigtableClient = BigtableClient.Create();
            var rowRange = RowRange.Closed(new BigtableByteString($"{pid}_"), new BigtableByteString($"{pid}_{long.MaxValue}"));
            var stream = bigtableClient.ReadRows(TableName.Parse(_tableName), RowSet.FromRowRanges(rowRange));

            var deleteRows = new List<MutateRowsRequest.Types.Entry>();

            using (var enumerator = stream.GetEnumerator())
            {
                while (enumerator.MoveNext().GetAwaiter().GetResult())
                {
                    deleteRows.Add(Mutations.CreateEntry(enumerator.Current.Key, Mutations.DeleteFromRow()));
                }
            }

            if (deleteRows.Any())
            {
                bigtableClient.MutateRows(TableName.Parse(_tableName), deleteRows);
            }
        }

        protected override bool SupportsRejectingNonSerializableObjects => true;

        protected override bool SupportsAtomicPersistAllOfSeveralEvents => true;
    }
}