using Akka.Configuration;
using Akka.TestKit.Xunit2;
using Xunit;

namespace Hafslund.Akka.Persistence.Bigtable.Tests
{
    public class BigtableSettingsTest : TestKit
    {
        [Fact]
        public void ShouldParseDefaultSnapshotConfig()
        {
            var snapshotSettings =
                BigtableSettings.Create(
                    ConfigurationFactory.ParseString(@"akka.persistence.snapshot-store.bigtable {
                        table-name = foo
                    }").WithFallback(BigtablePersistence.DefaultConfig)
                        .GetConfig("akka.persistence.snapshot-store.bigtable"));

            Assert.Equal("foo", snapshotSettings.TableName);
        }

        [Fact]
        public void BigtablePersistence_CustomJournalConfig_ShouldParseConfig()
        {
            var config = ConfigurationFactory.ParseString(
                            @"akka.persistence.journal.bigtable {
                                table-name = bar
                            }
                             akka.persistence.journal.bigtable-sharding {
                                table-name = foo
                            }")
                            .WithFallback(BigtablePersistence.DefaultConfig)
                            .WithFallback(ShardingBigtablePersistence.DefaultConfig);

            var journalSettings = BigtableSettings.Create(config.GetConfig("akka.persistence.journal.bigtable"));
            var shardingSettings = BigtableSettings.Create(config.GetConfig("akka.persistence.journal.bigtable-sharding"));

            Assert.Equal("bar", journalSettings.TableName);
            Assert.Equal("foo", shardingSettings.TableName);
        }

        [Fact]
        public void BigtablePersistence_CustomSnapshotConfig_ShouldParseConfig()
        {
            var config = ConfigurationFactory.ParseString(
                            @"akka.persistence.snapshot-store.bigtable {
                                table-name = bar
                            }
                             akka.persistence.snapshot-store.bigtable-sharding {
                                table-name = foo
                            }")
                            .WithFallback(BigtablePersistence.DefaultConfig)
                            .WithFallback(ShardingBigtablePersistence.DefaultConfig);

            var snapshotSettings = BigtableSettings.Create(config.GetConfig("akka.persistence.snapshot-store.bigtable"));
            var shardingSettings = BigtableSettings.Create(config.GetConfig("akka.persistence.snapshot-store.bigtable-sharding"));

            Assert.Equal("bar", snapshotSettings.TableName);
            Assert.Equal("foo", shardingSettings.TableName);
        }
    }
}
