using Akka.Actor;
using Akka.Configuration;

namespace Hafslund.Akka.Persistence.Bigtable
{
    public class BigtableTransportSerializationSettings
    {
        

        /// <summary>
        /// Fallback value for remote hostname when serializing local actor refs
        /// </summary>
        /// <example>127.0.0.1</example>
        public string Hostname { get; }

        /// <summary>
        /// Fallback value for remote port when serializing local actor refs
        /// </summary>
        /// <example>8091</example>
        public int Port { get; }

        /// <summary>
        /// Fallback value for remote protocol when serializing local actor refs
        /// </summary>
        /// <example>akka.tcp</example>
        public string TranportProtocol { get; }

        public Address GetFallbackAddress(IActorContext context)
        {
            return new Address(
                TranportProtocol,
                context.System.Name,
                Hostname,
                Port);
        }


        public BigtableTransportSerializationSettings(string hostname, int port, string tranportProtocol)
        {
            this.Hostname = hostname;
            this.Port = port;
            this.TranportProtocol = tranportProtocol;
        }

        public static BigtableTransportSerializationSettings Create(Config config)
        {
            return new BigtableTransportSerializationSettings(
                config.GetString("hostname"),
                config.GetInt("port"),
                config.GetString("transport-protocol")
            );
        }

    }
}