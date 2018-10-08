using System;
using Akka.Actor;
using Akka.Serialization;
using Newtonsoft.Json;

namespace Hafslund.Akka.Persistence.Bigtable.IntegrationTests
{
    public class ActorRefWrapperSerializer : Serializer
    {
        public ActorRefWrapperSerializer(ExtendedActorSystem system) : base(system)
        {
        }

        public override bool IncludeManifest => false;

        public override object FromBinary(byte[] bytes, Type type)
        {
            var actorRefString = System.Text.Encoding.Default.GetString(bytes);
            var path = ActorPath.Parse(actorRefString);
            var actorRef = system.Provider.ResolveActorRef(path);
            return new ActorRefWrapper()
            {
                ActorRef = actorRef,
                IsSerializedWithTransport = path.Address.HasGlobalScope
            };
        }

        public override byte[] ToBinary(object obj)
        {
            if (obj is ActorRefWrapper myEvent)
            {
                var actorPath = Serialization.SerializedActorPath(myEvent.ActorRef);
                return System.Text.Encoding.UTF8.GetBytes(actorPath.ToCharArray());
            }
            else
            {
                throw new NotSupportedException($"Cannot serialize type {obj.GetType()}");
            }
        }
    }
}