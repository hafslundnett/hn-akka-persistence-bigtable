using Akka.Configuration;
using Akka.TestKit.Xunit2;
using Hafslund.Akka.Persistence.Bigtable.Journal;
using Hafslund.Akka.Persistence.Bigtable.Snapshot;
using Xunit;

namespace Hafslund.Akka.Persistence.Bigtable.Tests
{
    public class BigtableSettingsTest : TestKit
    {
        [Fact]
        public void ShouldParseDefaultSnapshotConfig()
        {
            var snapshotSettings =
                BigtableSnapshotSettings.Create(
                    ConfigurationFactory.ParseString(@"akka.persistence.snapshot-store.bigtable {
                        table-name = foo
                        family-name = bar
                    }").WithFallback(BigtablePersistence.DefaultConfig)
                        .GetConfig("akka.persistence.snapshot-store.bigtable"));

            Assert.Equal("foo", snapshotSettings.TableName);
            Assert.Equal("bar", snapshotSettings.FamilyName);
        }

        [Fact]
        public void BigtablePersistence_CustomJournalConfig_ShouldParseConfig()
        {
            var config = ConfigurationFactory.ParseString(
                            @"akka.persistence.journal.bigtable {
                                table-name = bar
                                family-name = jalla
                            }
                             akka.persistence.journal.bigtable-sharding {
                                table-name = foo
                                family-name = baz
                            }")
                            .WithFallback(BigtablePersistence.DefaultConfig)
                            .WithFallback(ShardingBigtablePersistence.DefaultConfig);

            var journalSettings = BigtableJournalSettings.Create(config.GetConfig("akka.persistence.journal.bigtable"));
            var shardingSettings = BigtableJournalSettings.Create(config.GetConfig("akka.persistence.journal.bigtable-sharding"));

            Assert.Equal("bar", journalSettings.TableName);
            Assert.Equal("foo", shardingSettings.TableName);
            Assert.Equal("jalla", journalSettings.FamilyName);
            Assert.Equal("baz", shardingSettings.FamilyName);
        }

        [Fact]
        public void BigtablePersistence_CustomSnapshotConfig_ShouldParseConfig()
        {
            var config = ConfigurationFactory.ParseString(
                            @"akka.persistence.snapshot-store.bigtable {
                                table-name = bar
                                family-name = jalla
                            }
                             akka.persistence.snapshot-store.bigtable-sharding {
                                table-name = foo
                                family-name = baz
                            }")
                            .WithFallback(BigtablePersistence.DefaultConfig)
                            .WithFallback(ShardingBigtablePersistence.DefaultConfig);

            var snapshotSettings = BigtableSnapshotSettings.Create(config.GetConfig("akka.persistence.snapshot-store.bigtable"));
            var shardingSettings = BigtableSnapshotSettings.Create(config.GetConfig("akka.persistence.snapshot-store.bigtable-sharding"));

            Assert.Equal("bar", snapshotSettings.TableName);
            Assert.Equal("foo", shardingSettings.TableName);
            Assert.Equal("jalla", snapshotSettings.FamilyName);
            Assert.Equal("baz", shardingSettings.FamilyName);
        }
    }
}
