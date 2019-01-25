using Google.Cloud.Bigtable.V2;
using Grpc.Core;
using Hafslund.Akka.Persistence.Bigtable.Snapshot;

namespace Hafslund.Akka.Persistence.Bigtable.IntegrationTests.Snapshot
{
    public class BigtableSnapshotStoreTester : BigtableSnapshotStore
    {
        protected override BigtableClient CreateBigtableClient()
        {
            var channel = new Channel(BigtableSnapshotStoreSpec.Host, ChannelCredentials.Insecure);
            return BigtableClient.Create(BigtableServiceApiClient.Create(channel));
        }
    }
}