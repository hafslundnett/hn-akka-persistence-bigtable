using Akka.Actor;
using Akka.Configuration;
using Hafslund.Akka.Persistence.Bigtable.Journal;
using Hafslund.Akka.Persistence.Bigtable.Snapshot;

namespace Hafslund.Akka.Persistence.Bigtable
{
    /// <summary>
    ///     Used to configure the <see cref="BigtableJournal" />
    ///     and <see cref="BigtableSnapshotStore" />.
    /// </summary>
    public class BigtablePersistence : IExtension
    {
        public BigtablePersistence(BigtableJournalSettings bigtableJournalSettings, BigtableSnapshotSettings bigtableSnapshotSettings)
        {
            BigtableJournalSettings = bigtableJournalSettings;
            BigtableSnapshotSettings = bigtableSnapshotSettings;
        }

        public BigtableJournalSettings BigtableJournalSettings { get; }

        public BigtableSnapshotSettings BigtableSnapshotSettings { get; }

        /// <summary>
        ///     The default HOCON configuration for <see cref="BigtablePersistence" />.
        /// </summary>
        public static Config DefaultConfig =>
            ConfigurationFactory.FromResource<BigtablePersistence>("Hafslund.Akka.Persistence.Bigtable.reference.conf");

        /// <summary>
        ///     Returns the <see cref="BigtablePersistence" /> instance for <see cref="system" />.
        /// </summary>
        /// <param name="system">The current <see cref="ActorSystem" />.</param>
        /// <returns>
        ///     If <see cref="BigtablePersistence" /> has already been instantiated, gets the current instance. If not, creates a
        ///     new instance, registers it, and returns it.
        /// </returns>
        public static BigtablePersistence Get(ActorSystem system)
        {
            return system.WithExtension<BigtablePersistence, BigtablePersistenceProvider>();
        }
    }
}
