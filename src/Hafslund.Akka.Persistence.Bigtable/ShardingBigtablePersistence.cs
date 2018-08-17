using Akka.Actor;

namespace Hafslund.Akka.Persistence.Bigtable
{
    /// <summary>
    ///     Used to configure the <see cref="BigtableJournal" />
    ///     and <see cref="BigtableSnapshotStore" />.
    /// </summary>
    public sealed class ShardingBigtablePersistence : BigtablePersistence
    {
        public ShardingBigtablePersistence(BigtableSettings bigtableJournalSettings, BigtableSettings bigtableSnapshotSettings) : base(bigtableJournalSettings, bigtableSnapshotSettings)
        {
        }

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
