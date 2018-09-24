using System;
using System.Linq;
using System.Threading.Tasks;
using Akka.Actor;
using Akka.Configuration;
using AkkaIntegration.Tests.Performance.Persistence;
using Hafslund.Akka.Persistence.Bigtable.PerformanceTests.Journal;
using Microsoft.Extensions.Configuration;
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
            try
            {
                foreach (var pid in PersistentActorIds)
                {
                    DeleteRows(SnapshotStoreTable, pid);
                }
            }
            catch (Exception ex)
            {
                context.Trace.Error(ex, "Error occured during benchmark cleanup");
            }

            context.Trace.Info("Finished cleanup");
        }
    }
}