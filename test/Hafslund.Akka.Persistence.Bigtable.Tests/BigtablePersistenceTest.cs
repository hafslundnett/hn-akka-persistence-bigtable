using Akka.TestKit.Xunit2;
using System;
using System.Collections.Generic;
using System.Text;
using Xunit;

namespace Hafslund.Akka.Persistence.Bigtable.Tests
{
    public class BigtablePersistenceTest : TestKit
    {
        [Fact]
        public void BigtablePersistence_NoConfig_ShouldUseDefault()
        {
            var bigtablePersistence = BigtablePersistence.Get(Sys);

            Assert.Empty(bigtablePersistence.BigtableJournalSettings.TableName);
            Assert.Empty(bigtablePersistence.BigtableSnapshotSettings.TableName);
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
    }
}
