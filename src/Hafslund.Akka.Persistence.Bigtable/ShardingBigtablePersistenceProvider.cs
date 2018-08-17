using Akka.Actor;

namespace Hafslund.Akka.Persistence.Bigtable
{
    /// <summary>
    /// Used to instantiate the <see cref="ShardingBigtablePersistence" /> <see cref="ActorSystem" /> extension.
    /// </summary>
    public sealed class ShardingBigtablePersistenceProvider : ExtensionIdProvider<ShardingBigtablePersistence>
    {
        public override ShardingBigtablePersistence CreateExtension(ExtendedActorSystem system)
        {
            system.Settings.InjectTopLevelFallback(BigtablePersistence.DefaultConfig);

            var journalSettings =
                BigtableSettings.Create(
                    system.Settings.Config.GetConfig("akka.persistence.journal.bigtable-sharding"));

            var snapshotSettings =
                BigtableSettings.Create(
                    system.Settings.Config.GetConfig("akka.persistence.snapshot-store.bigtable-sharding"));

            return new ShardingBigtablePersistence(journalSettings, snapshotSettings);
        }
    }
}
