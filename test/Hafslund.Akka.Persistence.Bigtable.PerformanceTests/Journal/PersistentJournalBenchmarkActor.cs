using System;
using Akka.Actor;
using Akka.Persistence;

namespace AkkaIntegration.Tests.Performance.Persistence
{
    public class PersistentJournalBenchmarkActor : ReceivePersistentActor
    {
        /// <summary>
        /// Our stored value
        /// </summary>
        private int TotalCount { get; set; }

        private IActorRef _snapshotResultReplyTo;

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

            Command<PersistentBenchmarkMsgs.TakeSnapshot>(takeSnapshot =>
            {
                if (_snapshotResultReplyTo != null)
                {
                    throw new IllegalActorStateException("Already waiting for snapshot success");
                }
                _snapshotResultReplyTo = Sender;
                SaveSnapshot(new PersistentJournalBenchmarkActorState(takeSnapshot.Data, TotalCount));
            });

            Command<SaveSnapshotSuccess>(snapshotSuccessMessage =>
            {
                _snapshotResultReplyTo.Tell(PersistentBenchmarkMsgs.TookSnapshot.Instance);
                _snapshotResultReplyTo = null;
            });

            Command<SaveSnapshotFailure>(snapshotFailureMessage =>
            {
                _snapshotResultReplyTo.Tell(PersistentBenchmarkMsgs.SnapshotFailed.Instance);
                _snapshotResultReplyTo = null;
            });

            Recover<SnapshotOffer>(snapshotOffer => 
            {
            });

        }

        public override string PersistenceId { get; }
    }


    public class PersistentJournalBenchmarkActorState
    {
        public readonly object Data;

        public readonly int TotalCount;

        public PersistentJournalBenchmarkActorState(object data, int totalCount)
        {
            Data = data;
            TotalCount = totalCount;
        }
    }
}
