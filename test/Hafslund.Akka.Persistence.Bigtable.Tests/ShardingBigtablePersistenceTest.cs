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

            Assert.Empty(bigtablePersistence.BigtableJournalSettings.TableName);
            Assert.Empty(bigtablePersistence.BigtableSnapshotSettings.TableName);
        }

        [Fact]
        public void ShardingBigtablePersistence_Get_ShouldAddExtension()
        {
            var shardingBigtablePersistence = ShardingBigtablePersistence.Get(Sys);
            Assert.NotNull(shardingBigtablePersistence);
            Assert.True(Sys.HasExtension<ShardingBigtablePersistence>());
        }

        [Fact]
        public void ShardingBigtablePersistence_DefaultConfig_ShouldLoad()
        {
            var defaultConfig = BigtablePersistence.DefaultConfig;
            Assert.True(defaultConfig.HasPath("akka.persistence.journal.bigtable-sharding"));
            Assert.True(defaultConfig.HasPath("akka.persistence.snapshot-store.bigtable-sharding"));
        }
    }
}
