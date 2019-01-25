using System.Linq;
using System.Threading.Tasks;
using Akka.Actor;
using Hafslund.Akka.Persistence.Bigtable.PerformanceTests.Journal;
using NBench;

namespace Hafslund.Akka.Persistence.Bigtable.PerformanceTests.Snapshot
{
    public class BigtableSnapshotStoreLoadPerfSpec : BigtableSnapshotStorePerfSpec
    {
        public const string SnapshotLoadCounterName = "SnapshotLoads";
        protected Counter _snapshotLoadCounter;
        public const string RecoveryCounterName = "MsgRecovered";
        protected Counter _recoveryCounter;

        public override void Setup(BenchmarkContext context)
        {
            ReInitializeTable(SnapshotStoreTable);
            ReInitializeTable(JournalTable);

            base.Setup(context);
            _snapshotLoadCounter = context.GetCounter(SnapshotLoadCounterName);
            _recoveryCounter = context.GetCounter(RecoveryCounterName);
            StoreSnapshotForEachActor();
            TerminateAllActors();
        }

        [PerfBenchmark(NumberOfIterations = 5, RunMode = RunMode.Iterations,
            Description = "Snapshot load performance spec by 200 persistent actors", SkipWarmups = true)]
        [CounterMeasurement(SnapshotLoadCounterName)]
        [CounterMeasurement(RecoveryCounterName)]
        [GcMeasurement(GcMetric.TotalCollections, GcGeneration.AllGc)]
        [MemoryMeasurement(MemoryMetric.TotalBytesAllocated)]
        [TimingMeasurement]
        public void SnapshotLoadSpec(BenchmarkContext context)
        {
            var finished = new Task<PersistentBenchmarkMsgs.Finished>[PersistentActorCount];
            for (int i = 0; i < PersistentActorCount; i++)
            {
                var task = Supervisor
                    .Ask<PersistentBenchmarkMsgs.Finished>(new BenchmarkActorMessage(PersistentActorIds[i], PersistentBenchmarkMsgs.Finish.Instance), MaxTimeout);

                finished[i] = task;
            }

            Task.WaitAll(finished.Cast<Task>().ToArray());

            foreach (var task in finished.Where(x => x.IsCompleted))
            {
                _recoveryCounter.Increment(task.Result.State);
                _snapshotLoadCounter.Increment(1);
            }
        }

        protected Task<PersistentBenchmarkMsgs.TookSnapshot>[] StoreSnapshotForEachActor()
        {
            var finished = new Task<PersistentBenchmarkMsgs.TookSnapshot>[PersistentActorCount];
            for (int i = 0; i < PersistentActorCount; i++)
            {
                var msg = new BenchmarkActorMessage(PersistentActorIds[i], new PersistentBenchmarkMsgs.TakeSnapshot("snapshot"));
                var task = Supervisor
                    .Ask<PersistentBenchmarkMsgs.TookSnapshot>(msg, MaxTimeout);

                finished[i] = task;
            }

            Task.WaitAll(finished.Cast<Task>().ToArray());

            return finished;
        }
    }
}