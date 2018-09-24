using Akka.TestKit.Xunit2;
using Xunit;

namespace Hafslund.Akka.Persistence.Bigtable.Tests
{
    public class BigtablePersistenceTest : TestKit
    {
        [Fact]
        public void BigtablePersistence_NoConfig_ShouldUseDefault()
        {
            var bigtablePersistence = BigtablePersistence.Get(Sys);

            Assert.Empty(bigtablePersistence.JournalSettings.TableName);
            Assert.Empty(bigtablePersistence.SnapshotSettings.TableName);
            Assert.Equal("f", bigtablePersistence.JournalSettings.FamilyName);
            Assert.Equal("f", bigtablePersistence.SnapshotSettings.FamilyName);
        }

        [Fact]
        public void BigtablePersistence_Get_ShouldAddExtension()
        {
            var bigtablePersistence = BigtablePersistence.Get(Sys);
            Assert.NotNull(bigtablePersistence);
            Assert.True(Sys.HasExtension<BigtablePersistence>());
        }

        [Fact]
        public void BigtablePersistence_DefaultConfig_ShouldLoad()
        {
            var defaultConfig = BigtablePersistence.DefaultConfig;
            Assert.True(defaultConfig.HasPath("akka.persistence.journal.bigtable"));
            Assert.True(defaultConfig.HasPath("akka.persistence.snapshot-store.bigtable"));
        }

        [Fact]
        public void BigtablePersistence_UsingDedicatedShardingPlugin_ShouldUseBigtableShardingPlugin()
        {
            BigtablePersistence.Get(Sys);
            ShardingBigtablePersistence.Get(Sys);
            var clusterShardingJournalPlugin = Sys.Settings.Config.GetString("akka.cluster.sharding.journal-plugin-id");
            var clusterShardingSnapshotPlugin = Sys.Settings.Config.GetString("akka.cluster.sharding.snapshot-plugin-id");
            Assert.Equal("akka.persistence.journal.bigtable-sharding", clusterShardingJournalPlugin);
            Assert.Equal("akka.persistence.snapshot-store.bigtable-sharding", clusterShardingSnapshotPlugin);
        }
    }
}
