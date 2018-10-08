using Akka.Actor;

namespace Hafslund.Akka.Persistence.Bigtable.IntegrationTests
{
    public class ActorRefWrapper
    {
        public IActorRef ActorRef {get; set;}

        public bool IsSerializedWithTransport { get; set; }
    }
}