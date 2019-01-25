using System;
using System.Linq;
using System.Threading.Tasks;
using Akka.Actor;
using NBench;

namespace Hafslund.Akka.Persistence.Bigtable.PerformanceTests.Journal
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
            ReInitializeTable(JournalTable);
            context.Trace.Info("Finished cleanup");
        }
    }
}
