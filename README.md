# Hafslund.Akka.Persistence.Bigtable

## Installation
This plugin is (supposed to) be released publicly to nuget.org, and the latest released version can be installed by:
```CMD
dotnet add package Hafslund.Akka.Persistence.Bigtable
```

To use the plugin as an extension, add the following line of code to the Akka system initialization:
```C#
BigtablePersistence.Get(actorSystem);
```
(Note that this is similar to how the Cluster extension is set up, e.g. `Cluster.Get(actorSystem)`).

If it is preferred to load the extension from config instead of through code, specify the FQCNs for the Journal and Snasphot store classes in HOCON:

```HOCON
akka.persistence.journal.bigtable.class = "Hafslund.Akka.Persistence.Bigtable.Snapshot.BigtableJournal, Hafslund.Akka.Persistence.Bigtable"
akka.persistence.snapshot-store.bigtable.class = "Hafslund.Akka.Persistence.Bigtable.Snapshot.BigtableSnapshotStore, Hafslund.Akka.Persistence.Bigtable"
```

## Setup

The extension will load default configuration (see `reference.conf` from plugin source code), but as a minimum, the tablename has to be specified. There is no option to feed in a connection string for the table, as this is not how the Google Cloud SDK is set up to authenticate, see https://cloud.google.com/bigtable/docs/reference/libraries.

To activate the journal plugin, add the following lines to your actor system configuration file:
```HOCON
akka.persistence.journal.plugin = "akka.persistence.journal.bigtable"
akka.persistence.journal.bigtable.table-name = "your/bigtable/table/reference"
```

Similar configuration may be used to setup a Bigtable snapshot store:

```HOCON
akka.persistence.snapshot-store.plugin = "akka.persistence.snapshot.bigtable"
akka.persistence.snapshot-store.bigtable.table-name = "your/bigtable/table/reference"
```


Note that the tables with column family must be created before running this plugin, as there is no `auto-initialize` configuration setting provided (this option would require a different authorization level with create access to Bigtable, than what it is recommended that the akka system should run under). For a brief introduction on how to create tables and column families, see https://cloud.google.com/bigtable/docs/quickstart-cbt.

For a working CBT installation setup with a ~/.cbtrc file pointing to the right project and instance, the following set of commands gives an example:
```
cbt createtable MyJournalEvents
cbt createfamily MyJournalEvents f
cbt createtable MySnapshotStore
cbt createfamily MySnapshotStore f
```

Note that the family name `f` is the default configuration in the plugin if not otherwise specified. If the table is not created with a column family matching the configuration, nothing will be persisted. To override the default column family name in config, set the following HOCON config property:

```HOCON
akka.persistence.journal.bigtable.family-name = otherfamily
akka.persistence.snapshot-store.bigtable.family-name = otherfamily
```

## Sharding Setup

By default, sharding events and snapshots will be persisted by the same plugin by just specifying this in the sharding config:
```HOCON
akka.cluster.sharding {
	journal-plugin-id = "akka.persistence.journal.bigtable"
	snapshot-plugin-id = "akka.persistence.snapshot-store.bigtable"
	state-store-mode = persistence
}
```

If you want to instantiate a different instance for sharding events and snapshots (mainly if you would like to store these to separate tables), add the following line in code:
```C#
ShardingBigtablePersistence.Get(actorSystem);
```

If it is preferred to load the extension from config instead of through code, specify the FQCNs for the Journal and Snapshot steore classes specific for sharding in HOCON:

```HOCON
akka.persistence.journal.bigtable.class = "Hafslund.Akka.Persistence.Bigtable.Journal.ShardingBigtableJournal, Hafslund.Akka.Persistence.Bigtable"
akka.persistence.snapshot-store.bigtable.class = "Hafslund.Akka.Persistence.Bigtable.Snapshot.ShardingBigtableSnapshotStore, Hafslund.Akka.Persistence.Bigtable"
```


Then make sure the following changes to your HOCON:
```hocon
akka.cluster.sharding {
	journal-plugin-id = "akka.persistence.journal.bigtable-sharding"
	snapshot-plugin-id = "akka.persistence.snapshot-store.bigtable-sharding"
	state-store-mode = persistence
}

akka.persistence.journal.bigtable-sharding.table-name = "your/bigtable-sharding/table/reference/"
akka.persistence.snapshot-store.bigtable-sharding.table-name = "your/bigtable-sharding/table/reference"

```
If you are using a column family name other than the default `f`, remember to also set the following:

```hocon
akka.persistence.journal.bigtable-sharding.family-name = otherfamily
akka.persistence.snapshot-store.bigtable-sharding.family-name = otherfamily
```


## Configuration

Both journal and snapshot store share the same configuration keys (however they resides in separate scopes, so they are definied distinctly for either journal or snapshot store). Full configuration is given below (but note that minimal configuration is described above, and is mainly the table name).

```HOCON
akka.persistence {
  plugin = "akka.persistence.journal.bigtable"

  journal {
    bigtable {
	    # qualified type name of the Google Bigtable persistence journal actor
	    class = "Hafslund.Akka.Persistence.Bigtable.Journal.BigtableJournal, Hafslund.Akka.Persistence.Bigtable"

	    # the name of the Google Bigtable used to persist journal events
	    # (full name, i.e. 'projects/<project-id>/instances/<instance-id>/tables/<table-name> )
	    table-name = ""

	    # Column family name:
	    family-name = "f"

	    # dispatcher used to drive journal actor
	    plugin-dispatcher = "akka.actor.default-dispatcher"
    }

	bigtable-sharding {
	    # qualified type name of the Google Bigtable persistence journal actor
	    class = "Hafslund.Akka.Persistence.Bigtable.Journal.ShardingBigtableJournal, Hafslund.Akka.Persistence.Bigtable"

	    # the name of the Google Bigtable used to persist journal events
	    # (full name, i.e. 'projects/<project-id>/instances/<instance-id>/tables/<table-name> )
	    table-name = ""

	    # Column family name:
	    family-name = "f"
	    
	    # dispatcher used to drive journal actor
	    plugin-dispatcher = "akka.actor.default-dispatcher"
    }
  }  

  snapshot-store {
	plugin = "akka.persistence.snapshot-store.bigtable"
    
	bigtable {
	    # qualified type name of the Google Bigtable persistence snapshot storage actor
	    class = "Hafslund.Akka.Persistence.Bigtable.Snapshot.BigtableSnapshotStore, Hafslund.Akka.Persistence.Bigtable"

	    # the name of the Google Bigtable used to persist snapshots
	    # (full name, i.e. 'projects/<project-id>/instances/<instance-id>/tables/<table-name> )
	    table-name = ""
	    
	    # Column family name:
	    family-name = "f"

	    # dispatcher used to drive snapshot storage actor
	    plugin-dispatcher = "akka.actor.default-dispatcher"
    }

	bigtable-sharding {
	    # qualified type name of the Google Bigtable persistence snapshot storage actor
	    class = "Hafslund.Akka.Persistence.Bigtable.Snapshot.ShardingBigtableSnapshotStore, Hafslund.Akka.Persistence.Bigtable"

	    # the name of the Google Bigtable used to persist snapshots
	    # (full name, i.e. 'projects/<project-id>/instances/<instance-id>/tables/<table-name> )
	    table-name = ""

	    # Column family name:
	    family-name = "f"

	    # dispatcher used to drive snapshot storage actor
	    plugin-dispatcher = "akka.actor.default-dispatcher"
    }
  }
```