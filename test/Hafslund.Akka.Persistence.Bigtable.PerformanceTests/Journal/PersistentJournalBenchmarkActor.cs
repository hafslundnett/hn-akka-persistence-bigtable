using System;
using System.Collections.Generic;
using System.Text;
using Akka.Actor;
using Akka.Event;
using Akka.Persistence;

namespace AkkaIntegration.Tests.Performance.Persistence
{
    public class PersistentJournalBenchmarkActor : ReceivePersistentActor
    {
        private readonly ILoggingAdapter _log = Context.GetLogger();

        /// <summary>
        /// Our stored value
        /// </summary>
        private int TotalCount { get; set; }

        public PersistentJournalBenchmarkActor(string persistenceId)
        {
            PersistenceId = persistenceId;

            Recover<PersistentBenchmarkMsgs.Stored>(i =>
            {
                TotalCount += i.Value;
            });

            Command<PersistentBenchmarkMsgs.Store>(store =>
            {
                Persist(new PersistentBenchmarkMsgs.Stored(store.Value), s =>
                {
                    TotalCount += s.Value;
                });
            });

            Command<PersistentBenchmarkMsgs.Init>(i =>
            {
                var sender = Sender;
                Persist(new PersistentBenchmarkMsgs.Stored(0), s =>
                {
                    TotalCount += s.Value;
                    sender.Tell(PersistentBenchmarkMsgs.Done.Instance);
                });
            });

            Command<PersistentBenchmarkMsgs.Finish>(r =>
            {
                Sender.Tell(new PersistentBenchmarkMsgs.Finished(TotalCount));
            });
        }

        public override string PersistenceId { get; }
    }
}
