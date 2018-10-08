using Akka.TestKit.Xunit2;
using Xunit;

namespace Hafslund.Akka.Persistence.Bigtable.Tests
{
    public class ShardingBigtablePersistenceTest : TestKit
    {
        [Fact]
        public void ShardingBigtablePersistence_NoConfig_ShouldUseDefault()
        {
            var bigtablePersistence = ShardingBigtablePersistence.Get(Sys);

            Assert.Empty(bigtablePersistence.JournalSettings.TableName);
            Assert.Empty(bigtablePersistence.SnapshotSettings.TableName);
        }

        [Fact]
        public void ShardingBigtablePersistence_Get_ShouldAddExtension()
        {
            var shardingBigtablePersistence = ShardingBigtablePersistence.Get(Sys);
            Assert.NotNull(shardingBigtablePersistence);
            Assert.True(Sys.HasExtension<ShardingBigtablePersistence>());
            
            Assert.Equal("localhost", shardingBigtablePersistence.TransportSerializationSetttings.Hostname);
            Assert.Equal(2552, shardingBigtablePersistence.TransportSerializationSetttings.Port);
            Assert.Equal("akka.tcp", shardingBigtablePersistence.TransportSerializationSetttings.TranportProtocol);

            Assert.Equal("f", shardingBigtablePersistence.JournalSettings.FamilyName);
            Assert.Equal("f", shardingBigtablePersistence.SnapshotSettings.FamilyName);
        }

        [Fact]
        public void ShardingBigtablePersistence_DefaultConfig_ShouldLoad()
        {
            var defaultConfig = ShardingBigtablePersistence.DefaultConfig;
            Assert.True(defaultConfig.HasPath("akka.persistence.journal.bigtable-sharding"));
            Assert.True(defaultConfig.HasPath("akka.persistence.snapshot-store.bigtable-sharding"));
        }
    }
}
