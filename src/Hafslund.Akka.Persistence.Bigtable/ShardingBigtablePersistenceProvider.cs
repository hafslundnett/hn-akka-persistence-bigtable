﻿using Akka.Actor;
using Hafslund.Akka.Persistence.Bigtable.Journal;

namespace Hafslund.Akka.Persistence.Bigtable
{
    /// <summary>
    /// Used to instantiate the <see cref="ShardingBigtablePersistence" /> <see cref="ActorSystem" /> extension.
    /// </summary>
    public sealed class ShardingBigtablePersistenceProvider : ExtensionIdProvider<ShardingBigtablePersistence>
    {
        public override ShardingBigtablePersistence CreateExtension(ExtendedActorSystem system)
        {
            system.Settings.InjectTopLevelFallback(ShardingBigtablePersistence.DefaultConfig);

            var journalSettings =
                BigtableJournalSettings.Create(
                    system.Settings.Config.GetConfig("akka.persistence.journal.bigtable-sharding"));

            var snapshotSettings =
                BigtableSnasphotSettings.Create(
                    system.Settings.Config.GetConfig("akka.persistence.snapshot-store.bigtable-sharding"));

            return new ShardingBigtablePersistence(journalSettings, snapshotSettings);
        }
    }
}
