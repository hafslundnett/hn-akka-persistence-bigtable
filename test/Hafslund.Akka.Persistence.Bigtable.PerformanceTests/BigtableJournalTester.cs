using Google.Cloud.Bigtable.V2;
using Grpc.Core;
using Hafslund.Akka.Persistence.Bigtable.Journal;

namespace Hafslund.Akka.Persistence.Bigtable.PerformanceTests
{
    public class BigtableJournalTester : BigtableJournal
    {
        protected override BigtableClient CreateBigtableClient()
        {
            var channel = new Channel(BigtablePluginPerfSpec.Host, ChannelCredentials.Insecure);
            return BigtableClient.Create(BigtableServiceApiClient.Create(channel));
        }
    }
}