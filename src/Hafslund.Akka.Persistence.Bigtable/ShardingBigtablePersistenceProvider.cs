using Akka.Actor;
using Hafslund.Akka.Persistence.Bigtable.Journal;
using Hafslund.Akka.Persistence.Bigtable.Snapshot;

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
                BigtableSnapshotSettings.Create(
                    system.Settings.Config.GetConfig("akka.persistence.snapshot-store.bigtable-sharding"));
            
            var transportSerializationSetttings = 
                BigtableTransportSerializationSettings.Create(
                    system.Settings.Config.GetConfig("akka.persistence.transport-serialization.bigtable"));

            return new ShardingBigtablePersistence(journalSettings, snapshotSettings, transportSerializationSetttings);
        }
    }
}
