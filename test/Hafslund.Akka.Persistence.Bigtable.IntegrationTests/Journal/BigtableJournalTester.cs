using Google.Cloud.Bigtable.V2;
using Grpc.Core;
using Hafslund.Akka.Persistence.Bigtable.Journal;

namespace Hafslund.Akka.Persistence.Bigtable.IntegrationTests.Journal
{
    public class BigtableJournalTester : BigtableJournal
    {
        protected override BigtableClient CreateBigtableClient()
        {
            var channel = new Channel(BigtableJournalSpec.Host, ChannelCredentials.Insecure);
            return BigtableClient.Create(BigtableServiceApiClient.Create(channel));
        }
    }
}