using System;
using System.Linq;
using Akka.Actor;
using NBench;

namespace Hafslund.Akka.Persistence.Bigtable.PerformanceTests.Journal
{
    public class BigtableJournalRecoverPerfSpec : BigTableJournalPerfSpecs
    {
        public const string RecoveryCounterName = "MsgRecovered";
        protected Counter _recoveryCounter;

        public override void Setup(BenchmarkContext context)
        {
            ReInitializeTable(JournalTable);
            base.Setup(context);
            _recoveryCounter = context.GetCounter(RecoveryCounterName);
            StoreAllEvents();
            Supervisor.Ask<AllTerminated>(new TerminateAll(), TimeSpan.FromSeconds(10)).GetAwaiter().GetResult();
        }

        [PerfBenchmark(NumberOfIterations = 5, RunMode = RunMode.Iterations,
            Description = "Recovery performance spec by 200 persistent actors", SkipWarmups = true)]
        [CounterMeasurement(RecoveryCounterName)]
        [GcMeasurement(GcMetric.TotalCollections, GcGeneration.AllGc)]
        [MemoryMeasurement(MemoryMetric.TotalBytesAllocated)]
        [TimingMeasurement]
        public void JournalRecoverySpec(BenchmarkContext context)
        {
            var recovered = RecoverAllEvents();
            foreach (var task in recovered.Where(x => x.IsCompleted))
            {
                _recoveryCounter.Increment(task.Result.State);
            }
        }
    }
}