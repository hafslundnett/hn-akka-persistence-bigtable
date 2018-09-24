using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Akka.Actor;
using AkkaIntegration.Tests.Performance.Persistence;

namespace Hafslund.Akka.Persistence.Bigtable.PerformanceTests.Journal
{
    public class BenchmarkActorSupervisor : ReceiveActor
    {

        private readonly IDictionary<string, IActorRef> _persistentActors = new Dictionary<string, IActorRef>();

        private IActorRef _terminatedRespondTo;

        public BenchmarkActorSupervisor()
        {
            Receive<BenchmarkActorMessage>(msg =>
            {
                IActorRef actorRef;
                if (_persistentActors.ContainsKey(msg.Id))
                {
                    actorRef = _persistentActors[msg.Id];
                }
                else
                {
                    actorRef = Context.ActorOf(Props.Create(() => new PersistentJournalBenchmarkActor(msg.Id)), msg.Id);
                    _persistentActors[msg.Id] = Context.Watch(actorRef);
                }
                actorRef.Forward(msg.Payload);
            });

            Receive<TerminateAll>(msg =>
            {
                if (_terminatedRespondTo != null)
                {
                    throw new IllegalActorStateException("");
                }
                _terminatedRespondTo = Sender;
                foreach (var actorRef in _persistentActors.Values)
                {
                    actorRef.GracefulStop(TimeSpan.FromSeconds(5));
                }
            });

            Receive<Terminated>(msg =>
            {
                var name = msg.ActorRef.Path.Name;
                _persistentActors.Remove(name);

                if (_persistentActors.Count == 0)
                {
                    _terminatedRespondTo.Tell(new AllTerminated());
                    _terminatedRespondTo = null;
                }
            });
        }
    }

    public class TerminateAll { }

    public class AllTerminated { }

    public class BenchmarkActorMessage
    {
        public BenchmarkActorMessage(string id, object payload)
        {
            this.Id = id;
            this.Payload = payload;
        }
        public string Id { get; }
        public object Payload { get; }

    }
}