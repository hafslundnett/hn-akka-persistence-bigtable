using Akka.Actor;

namespace Hafslund.Akka.Persistence.Bigtable
{
    /// <summary>
    /// Used to instantiate the <see cref="BigtablePersistence" /> <see cref="ActorSystem" /> extension.
    /// </summary>
    public sealed class BigtablePersistenceProvider : ExtensionIdProvider<BigtablePersistence>
    {
        public override BigtablePersistence CreateExtension(ExtendedActorSystem system)
        {
            system.Settings.InjectTopLevelFallback(BigtablePersistence.DefaultConfig);

            // TODO: Consider using the "akka.persistence.journal.plugin", which in turn points to the configured journal plugin, where we can configure tablename separately:
            var journalSettings =
                BigtableSettings.Create(
                    system.Settings.Config.GetConfig("akka.persistence.journal.bigtable"));

            // TODO: Consider using the "akka.persistence.snapshot.plugin", which in turn points to the configured snapshot plugin, where we can configure tablename separately:
            var snapshotSettings =
                BigtableSettings.Create(
                    system.Settings.Config.GetConfig("akka.persistence.snapshot-store.bigtable"));

            return new BigtablePersistence(system, journalSettings, snapshotSettings);
        }
    }
}
