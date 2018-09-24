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
using Hafslund.Akka.Persistence.Bigtable.PerformanceTests;

namespace AkkaIntegration.Tests.Performance.Persistence
{
    public class BigTableJournalPerfSpecs : BigtablePluginPerfSpec
    {
        protected const int PersistedMessageCount = 100;

        protected Task<PersistentBenchmarkMsgs.Finished>[] StoreAllEvents()
        {
            for (int i = 0; i < PersistedMessageCount; i++)
                for (int j = 0; j < PersistentActorCount; j++)
                {
                    Supervisor.Tell(new BenchmarkActorMessage(PersistentActorIds[j], new PersistentBenchmarkMsgs.Store(1)));
                }

            var finished = new Task<PersistentBenchmarkMsgs.Finished>[PersistentActorCount];
            for (int i = 0; i < PersistentActorCount; i++)
            {
                var task = Supervisor
                    .Ask<PersistentBenchmarkMsgs.Finished>(new BenchmarkActorMessage(PersistentActorIds[i], PersistentBenchmarkMsgs.Finish.Instance), MaxTimeout);

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
                var task = Supervisor
                    .Ask<PersistentBenchmarkMsgs.Finished>(new BenchmarkActorMessage(PersistentActorIds[i], PersistentBenchmarkMsgs.Finish.Instance), MaxTimeout);

                recovered[i] = task;
            }

            Task.WaitAll(recovered.Cast<Task>().ToArray());
            return recovered;
        }

        [PerfCleanup]
        public void CleanUp(BenchmarkContext context)
        {
            context.Trace.Info("Started cleanup");
            ActorSystem.Terminate().Wait();
            try
            {
                foreach (var pid in PersistentActorIds)
                {
                    DeleteRows(JournalTable, pid);
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
