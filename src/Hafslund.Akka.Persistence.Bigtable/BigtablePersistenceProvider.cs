using Akka.Actor;

namespace Hafslund.Akka.Persistence.Bigtable
{
    /// <summary>
    /// Used to instantiate the <see cref="BigtablePersistence" /> <see cref="ActorSystem" /> extension.
    /// </summary>
    public sealed class BigtablePersistenceProvider : ExtensionIdProvider<BigtablePersistence>
    {
        private const string JournalPluginSettingName = "akka.persistence.journal.plugin";
        private const string SnapshotPluginSettingName = "akka.persistence.snapshot-store.plugin";

        public override BigtablePersistence CreateExtension(ExtendedActorSystem system)
        {
            system.Settings.InjectTopLevelFallback(BigtablePersistence.DefaultConfig);

            // Note that this allows for naming the plugin in the HOCON config whatever you like
            var journalPluginName = system.Settings.Config.GetString(JournalPluginSettingName);
            var journalSettings =
                BigtableSettings.Create(
                    system.Settings.Config.GetConfig(journalPluginName));

            var snapshotPluginName = system.Settings.Config.GetString(SnapshotPluginSettingName);
            var snapshotSettings =
                BigtableSettings.Create(
                    system.Settings.Config.GetConfig(snapshotPluginName));

            return new BigtablePersistence(journalSettings, snapshotSettings);
        }
    }
}
