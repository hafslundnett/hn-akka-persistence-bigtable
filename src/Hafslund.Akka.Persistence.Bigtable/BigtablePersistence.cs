using Akka.Actor;
using Akka.Configuration;
using System;
using System.Collections.Generic;
using System.Text;

namespace Hafslund.Akka.Persistence.Bigtable
{
    /// <summary>
    ///     Used to configure the <see cref="BigtableJournal" />
    ///     and <see cref="BigtableSnapshotStore" />.
    /// </summary>
    public sealed class BigtablePersistence : IExtension
    {
        public BigtablePersistence(BigtableSettings bigtableJournalSettings, BigtableSettings bigtableSnapshotSettings)
        {
            BigtableJournalSettings = bigtableJournalSettings;
            BigtableSnapshotSettings = bigtableSnapshotSettings;
        }

        public BigtableSettings BigtableJournalSettings { get; }

        public BigtableSettings BigtableSnapshotSettings { get; }

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
