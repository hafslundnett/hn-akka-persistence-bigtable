using System;
using NBench;

namespace Hafslund.Akka.Persistence.Bigtable.PerformanceTests.Snapshot
{
    public class BigtableSnapshotStorePerfSpec : BigtablePluginPerfSpec
    {
        
        [PerfCleanup]
        public void CleanUp(BenchmarkContext context)
        {
            context.Trace.Info("Started cleanup");
            ActorSystem.Terminate().Wait();
            ReInitializeTable(SnapshotStoreTable);
            ReInitializeTable(JournalTable);
            context.Trace.Info("Finished cleanup");
        }
    }
}