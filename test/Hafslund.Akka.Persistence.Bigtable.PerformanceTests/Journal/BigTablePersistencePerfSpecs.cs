using Akka.Actor;
using Akka.Configuration;
using Akka.Util.Internal;
using Google.Cloud.Bigtable.Common.V2;
using Google.Cloud.Bigtable.V2;
using Hafslund.Akka.Persistence.Bigtable.PerformanceTests.Journal;
using NBench;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Microsoft.Extensions.Configuration;

namespace AkkaIntegration.Tests.Performance.Persistence
{
    public class BigTableJournalPerfSpecs
    {
        public static AtomicCounter TableVersionCounter = new AtomicCounter(0);
        private static string BigtableTableName;
        private static readonly Config SpecConfig;

        public static IConfigurationRoot ReadConfig()
        {
            return new ConfigurationBuilder()
                .AddJsonFile("appsettings.Development.json", optional: true)
                .AddEnvironmentVariables()
                .Build();
        }

        static BigTableJournalPerfSpecs()
        {
            var config = ReadConfig();
            BigtableTableName = config.GetValue<string>("PERFORMANCE_TEST_JOURNAL_TABLE");

            SpecConfig = ConfigurationFactory.ParseString(@"
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
                        auto-start-journals = [""akka.persistence.journal.bigtable""]
                        publish-plugin-commands = on
                        journal {
                            plugin = ""akka.persistence.journal.bigtable""
                            bigtable {
                                class = ""Hafslund.Akka.Persistence.Bigtable.Journal.BigtableJournal, Hafslund.Akka.Persistence.Bigtable""
                                plugin-dispatcher = ""akka.actor.default-dispatcher""
                                table-name = """ + BigtableTableName + @"""
                                auto-initialize = on
                                default-serializer = messagepack
                            }
                        }
                    }
                }");
        }

        public const int PersistentActorCount = 200;
        public const int PersistedMessageCount = 100;
        public static readonly TimeSpan MaxTimeout = TimeSpan.FromMinutes(6);
        private ActorSystem ActorSystem { get; set; }
        protected IActorRef _supervisor;
        protected readonly List<String> _persistentActorIds = new List<String>(PersistentActorCount);

        [PerfSetup]
        public virtual void Setup(BenchmarkContext context)
        {

            ActorSystem = ActorSystem.Create(nameof(BigTableJournalPerfSpecs) + TableVersionCounter.Current, SpecConfig);

            _supervisor = ActorSystem.ActorOf<BenchmarkActorSupervisor>("supervisor");

            foreach (var i in Enumerable.Range(0, PersistentActorCount))
            {
                var id = "persistent" + Guid.NewGuid();
                _persistentActorIds.Add(id);
            }

            // force the system to initialize
            Task.WaitAll(_persistentActorIds.Select(id => _supervisor.Ask<PersistentBenchmarkMsgs.Done>(new BenchmarkActorMessage(id, PersistentBenchmarkMsgs.Init.Instance))).Cast<Task>().ToArray());
            AfterSetup();
        }

        protected virtual void AfterSetup() { }

        protected Task<PersistentBenchmarkMsgs.Finished>[] StoreAllEvents()
        {
            for (int i = 0; i < PersistedMessageCount; i++)
                for (int j = 0; j < PersistentActorCount; j++)
                {
                    _supervisor.Tell(new BenchmarkActorMessage(_persistentActorIds[j], new PersistentBenchmarkMsgs.Store(1)));
                }

            var finished = new Task<PersistentBenchmarkMsgs.Finished>[PersistentActorCount];
            for (int i = 0; i < PersistentActorCount; i++)
            {
                var task = _supervisor
                    .Ask<PersistentBenchmarkMsgs.Finished>(new BenchmarkActorMessage(_persistentActorIds[i], PersistentBenchmarkMsgs.Finish.Instance), MaxTimeout);

                finished[i] = task;
            }

            Task.WaitAll(finished.Cast<Task>().ToArray());

            return finished;
        }

        protected Task<PersistentBenchmarkMsgs.Finished>[] RecoverAllEvents()
        {
            var recovered = new Task<PersistentBenchmarkMsgs.Finished>[PersistentActorCount];
            for (int i = 0; i < PersistentActorCount; i++)
            {
                var task = _supervisor
                    .Ask<PersistentBenchmarkMsgs.Finished>(new BenchmarkActorMessage(_persistentActorIds[i], PersistentBenchmarkMsgs.Finish.Instance), MaxTimeout);

                recovered[i] = task;
            }

            Task.WaitAll(recovered.Cast<Task>().ToArray());
            return recovered;
        }

        [PerfCleanup]
        public void CleanUp(BenchmarkContext context)
        {
            ActorSystem.Terminate().Wait();

            try
            {
                var bigtableClient = BigtableClient.Create();
                var stream = bigtableClient.ReadRows(TableName.Parse(BigtableTableName));
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
                    bigtableClient.MutateRows(TableName.Parse(BigtableTableName), deleteRows);
                }
            }
            catch (Exception ex)
            {
                context.Trace.Error(ex, "Error occured during benchmark cleanup");
            }
        }
    }
}
