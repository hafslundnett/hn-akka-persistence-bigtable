using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Akka.Actor;
using Akka.Configuration;
using AkkaIntegration.Tests.Performance.Persistence;
using Google.Cloud.Bigtable.Common.V2;
using Google.Cloud.Bigtable.V2;
using Hafslund.Akka.Persistence.Bigtable.PerformanceTests.Journal;
using Microsoft.Extensions.Configuration;
using NBench;
using NBench.Util;

namespace Hafslund.Akka.Persistence.Bigtable.PerformanceTests
{
    public class BigtablePluginPerfSpec
    {
        private static Config GetSpecConfig(IConfigurationRoot config, string snapshotTable, string journalTable)
        {
            var specConfig = ConfigurationFactory.ParseString(@"
                akka {
                    actor {
                        serializers {
                            messagepack = ""Akka.Serialization.MessagePack.MsgPackSerializer, Akka.Serialization.MessagePack""
                        }

                        serialization-bindings {
                            ""System.Object"" = messagepack
                        }
                    }

                    persistence {
                        publish-plugin-commands = on
                        snapshot-store {
                            plugin = ""akka.persistence.snapshot-store.bigtable""
                            bigtable {
                                class = ""Hafslund.Akka.Persistence.Bigtable.Snapshot.BigtableSnapshotStore, Hafslund.Akka.Persistence.Bigtable""
                                plugin-dispatcher = ""akka.actor.default-dispatcher""
                                table-name = """ + snapshotTable + @"""
                                auto-initialize = on
                                default-serializer = messagepack
                            }
                        }

                        journal {
                            plugin = ""akka.persistence.journal.bigtable""
                            bigtable {
                                class = ""Hafslund.Akka.Persistence.Bigtable.Journal.BigtableJournal, Hafslund.Akka.Persistence.Bigtable""
                                plugin-dispatcher = ""akka.actor.default-dispatcher""
                                table-name = """ + journalTable + @"""
                                auto-initialize = on
                                default-serializer = messagepack
                            }
                        }
                    }
                }");

            return specConfig;
        }
        public string SnapshotStoreTable { get; }
        public string JournalTable { get; }
        private readonly Config _specConfig;
        public static AtomicCounter TableVersionCounter = new AtomicCounter(0);
        public static IConfigurationRoot ReadConfig()
        {
            return new ConfigurationBuilder()
                .AddJsonFile("appsettings.Development.json", optional: true)
                .AddEnvironmentVariables()
                .Build();
        }

        protected virtual int PersistentActorCount { get; } = 200;
        public static readonly TimeSpan MaxTimeout = TimeSpan.FromMinutes(6);
        protected ActorSystem ActorSystem { get; set; }
        protected IActorRef Supervisor { get; set; }
        private readonly List<String> _persistentActorIds = new List<String>();
        protected List<String> PersistentActorIds => _persistentActorIds;

        protected BigtablePluginPerfSpec()
        {
            var config = ReadConfig();
            SnapshotStoreTable = config.GetValue<string>("PERFORMANCE_TEST_SNAPSHOT_STORE_TABLE");
            JournalTable = config.GetValue<string>("PERFORMANCE_TEST_JOURNAL_TABLE");
            _specConfig = GetSpecConfig(config, SnapshotStoreTable, JournalTable);
        }

        [PerfSetup]
        public virtual void Setup(BenchmarkContext context)
        {
            ActorSystem = ActorSystem.Create(GetType().Name + TableVersionCounter.Current, _specConfig);

            Supervisor = ActorSystem.ActorOf<BenchmarkActorSupervisor>("supervisor");

            foreach (var i in Enumerable.Range(0, PersistentActorCount))
            {
                var id = "persistent" + Guid.NewGuid();
                _persistentActorIds.Add(id);
            }

            // force the system to initialize
            WakeUpAllActors();
        }


        protected void WakeUpAllActors()
        {
            Task.WaitAll(_persistentActorIds.Select(id => Supervisor.Ask<PersistentBenchmarkMsgs.Done>(new BenchmarkActorMessage(id, PersistentBenchmarkMsgs.Init.Instance))).Cast<Task>().ToArray());
        }

        protected void TerminateAllActors()
        {
            Supervisor.Ask<AllTerminated>(new TerminateAll(), TimeSpan.FromMinutes(2)).GetAwaiter().GetResult();
        }

        protected void DeleteRows(string tableName, string pid)
        {

            var rowRange = RowRange.Closed(new BigtableByteString($"{pid}"), new BigtableByteString($"{pid}~"));
            var bigtableClient = BigtableClient.Create();
            var stream = bigtableClient.ReadRows(TableName.Parse(tableName), RowSet.FromRowRanges(rowRange));

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
                bigtableClient.MutateRows(TableName.Parse(tableName), deleteRows);
            }
        }

    }
}