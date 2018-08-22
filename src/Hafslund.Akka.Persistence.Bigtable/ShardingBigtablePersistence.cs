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
    public sealed class ShardingBigtablePersistence : BigtablePersistence
    {
        public ShardingBigtablePersistence(BigtableJournalSettings bigtableJournalSettings, BigtableSnapshotSettings bigtableSnapshotSettings) : base(bigtableJournalSettings, bigtableSnapshotSettings)
        {
        }

        /// <summary>
        ///     The default HOCON configuration for <see cref="ShardingBigtablePersistence" />.
        /// </summary>
        public static new Config DefaultConfig =>
            ConfigurationFactory.FromResource<ShardingBigtablePersistence>("Hafslund.Akka.Persistence.Bigtable.reference-sharding.conf");

        /// <summary>
        ///     Returns the <see cref="ShardingBigtablePersistence" /> instance for <see cref="system" />.
        /// </summary>
        /// <param name="system">The current <see cref="ActorSystem" />.</param>
        /// <returns>
        ///     If <see cref="ShardingBigtablePersistence" /> has already been instantiated, gets the current instance. If not, creates a
        ///     new instance, registers it, and returns it.
        /// </returns>
        public static new ShardingBigtablePersistence Get(ActorSystem system)
        {
            return system.WithExtension<ShardingBigtablePersistence, ShardingBigtablePersistenceProvider>();
        }
    }
}
