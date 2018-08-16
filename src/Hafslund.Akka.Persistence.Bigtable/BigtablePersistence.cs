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
        private readonly ActorSystem _system;

        public BigtablePersistence(ActorSystem system, BigtableSettings bigtableJournalSettings, BigtableSettings bigtableSnapshotSettings)
        {
            _system = system;
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
    }
}
