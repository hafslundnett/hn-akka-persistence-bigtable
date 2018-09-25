using Akka.Actor;
using Hafslund.Akka.Persistence.Bigtable.Journal;
using Hafslund.Akka.Persistence.Bigtable.Snapshot;

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

            var journalSettings =
                BigtableJournalSettings.Create(
                    system.Settings.Config.GetConfig("akka.persistence.journal.bigtable"));

            var snapshotSettings =
                BigtableSnapshotSettings.Create(
                    system.Settings.Config.GetConfig("akka.persistence.snapshot-store.bigtable"));

            return new BigtablePersistence(journalSettings, snapshotSettings);
        }
    }
}
