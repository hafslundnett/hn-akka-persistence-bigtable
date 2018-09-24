using System.Linq;
using AkkaIntegration.Tests.Performance.Persistence;
using NBench;

namespace Hafslund.Akka.Persistence.Bigtable.PerformanceTests.Journal
{
    public class BigtableJournalWritePerfSpec : BigTableJournalPerfSpecs
    {
        public const string WriteCounterName = "MsgPersisted";
        protected Counter _writeCounter;

        public override void Setup(BenchmarkContext context)
        {
            base.Setup(context);
            _writeCounter = context.GetCounter(WriteCounterName);
        }


        [PerfBenchmark(NumberOfIterations = 5, RunMode = RunMode.Iterations,
            Description = "Write performance spec by 200 persistent actors", SkipWarmups = true)]
        [CounterMeasurement(WriteCounterName)]
        //[CounterMeasurement(RecoveryCounterName)]
        [GcMeasurement(GcMetric.TotalCollections, GcGeneration.AllGc)]
        [MemoryMeasurement(MemoryMetric.TotalBytesAllocated)]
        [TimingMeasurement]
        public void JournalWriteSpec(BenchmarkContext context)
        {
            var finished = StoreAllEvents();

            foreach (var task in finished.Where(x => x.IsCompleted))
            {
                _writeCounter.Increment(task.Result.State);
            }
        }
    }
}