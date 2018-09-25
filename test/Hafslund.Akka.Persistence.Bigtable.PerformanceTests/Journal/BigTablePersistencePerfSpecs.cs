using Akka.Actor;
using Akka.Configuration;
using Akka.Util.Internal;
using Google.Cloud.Bigtable.Common.V2;
using Google.Cloud.Bigtable.V2;
using NBench;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace AkkaIntegration.Tests.Performance.Persistence
{
    public class BigTableJournalPerfSpecs
    {
        public const string RecoveryCounterName = "MsgRecovered";
        private Counter _recoveryCounter;

        public const string WriteCounterName = "MsgPersisted";
        private Counter _writeCounter;

        public static AtomicCounter TableVersionCounter = new AtomicCounter(0);

        private static string _tableName = Environment.GetEnvironmentVariable("PERFORMANCE_TEST_JOURNAL_TABLE");

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


        public const int PersistentActorCount = 200;
        public const int PersistedMessageCount = 20;

        public static readonly TimeSpan MaxTimeout = TimeSpan.FromMinutes(6);

        private ActorSystem ActorSystem { get; set; }

        private readonly List<IActorRef> _persistentActors = new List<IActorRef>(PersistentActorCount);

        [PerfSetup]
        public void Setup(BenchmarkContext context)
        {
            _recoveryCounter = context.GetCounter(RecoveryCounterName);
            _writeCounter = context.GetCounter(WriteCounterName);


            ActorSystem = ActorSystem.Create(nameof(BigTableJournalPerfSpecs) + TableVersionCounter.Current, SpecConfig);

            foreach (var i in Enumerable.Range(0, PersistentActorCount))
            {
                var id = "persistent" + Guid.NewGuid();
                var actorRef =
                    ActorSystem.ActorOf(
                        Props.Create(() => new PersistentJournalBenchmarkActor(id)),
                        id);

                _persistentActors.Add(actorRef);
            }

            // force the system to initialize
            Task.WaitAll(_persistentActors.Select(a => a.Ask<PersistentBenchmarkMsgs.Done>(PersistentBenchmarkMsgs.Init.Instance)).Cast<Task>().ToArray());
        }

        [PerfBenchmark(NumberOfIterations = 5, RunMode = RunMode.Iterations,
            Description = "Write performance spec by 200 persistent actors", SkipWarmups = true)]
        [CounterMeasurement(RecoveryCounterName)]
        [CounterMeasurement(WriteCounterName)]
        [GcMeasurement(GcMetric.TotalCollections, GcGeneration.AllGc)]
        [MemoryMeasurement(MemoryMetric.TotalBytesAllocated)]
        [TimingMeasurement]
        public void BatchJournalWriteSpec(BenchmarkContext context)
        {
            for (int i = 0; i < PersistedMessageCount; i++)
                for (int j = 0; j < PersistentActorCount; j++)
                {
                    _persistentActors[j].Tell(new PersistentBenchmarkMsgs.Store(1));
                }

            var finished = new Task<PersistentBenchmarkMsgs.Finished>[PersistentActorCount];
            for (int i = 0; i < PersistentActorCount; i++)
            {
                var task = _persistentActors[i]
                    .Ask<PersistentBenchmarkMsgs.Finished>(PersistentBenchmarkMsgs.Finish.Instance, MaxTimeout);

                finished[i] = task;
            }

            Task.WaitAll(finished.Cast<Task>().ToArray());
            foreach (var task in finished.Where(x => x.IsCompleted))
            {
                _writeCounter.Increment(task.Result.State);
            }
        }

        [PerfCleanup]
        public void CleanUp(BenchmarkContext context)
        {
            ActorSystem.Terminate().Wait();

            try
            {
                var bigtableClient = BigtableClient.Create();
                var stream = bigtableClient.ReadRows(TableName.Parse(_tableName));

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
            catch (Exception ex)
            {
                context.Trace.Error(ex, "Error occured during benchmark cleanup");
            }
        }
    }
}
