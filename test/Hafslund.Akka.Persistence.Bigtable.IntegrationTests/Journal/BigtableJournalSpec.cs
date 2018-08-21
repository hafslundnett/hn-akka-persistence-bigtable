using System.Collections.Generic;
using System.Linq;
using Akka.Persistence.TCK.Journal;
using Google.Cloud.Bigtable.Common.V2;
using Google.Cloud.Bigtable.V2;
using Akka.Configuration;
using System;
using Hafslund.Akka.Persistence.Bigtable.IntegrationTests;
using Microsoft.Extensions.Configuration;
using Xunit;

namespace Hafslund.Akka.Persistence.Bigtable.Tests.Integration.Journal
{
    public class BigtableJournalSpec : JournalSpec
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

        static BigtableJournalSpec()
        {
            var config = ReadConfig();

            var timeFactor= int.Parse(config.GetValue("INTEGRATION_TEST_TIME_FACTOR", "1"));
            Console.WriteLine($"BigtableSnapshotStoreSpec timefactor: {timeFactor}");

            TableName = config.GetValue("INTEGRATION_TEST_JOURNAL_TABLE", "NOT_SET");
            Console.WriteLine($"BigtableJournalSpec bigtable table: {TableName}");

            SpecConfig = ConfigurationFactory.ParseString($"akka.test.timefactor={timeFactor}")
                .WithFallback(ConfigurationFactory.ParseString(@"
                akka {
                    serializers {
                        messagepack = ""Akka.Serialization.MessagePack.MsgPackSerializer, Akka.Serialization.MessagePack""
                    }
                    persistence {
                        publish-plugin-commands = on
                        journal {
                            plugin = ""akka.persistence.journal.bigtable""
                            bigtable {
                                class = ""Hafslund.Akka.Persistence.Bigtable.Journal.BigtableJournal, Hafslund.Akka.Persistence.Bigtable""
                                plugin-dispatcher = ""akka.actor.default-dispatcher""
                                table-name = """ + TableName + @"""
                                auto-initialize = on
                            }
                        }
                    }
                }"));
        }
        public BigtableJournalSpec() : base(SpecConfig)
        {
            Initialize();
        }

        protected override void PreparePersistenceId(string pid)
        {
            var rowRange = RowRange.Closed(new BigtableByteString($"{pid}"), new BigtableByteString($"{pid}~"));
            BigtableTestUtils.DeleteRows(TableName, rowRange);
        }

        protected override bool SupportsRejectingNonSerializableObjects => true;

        protected override bool SupportsAtomicPersistAllOfSeveralEvents => true;
    }
}