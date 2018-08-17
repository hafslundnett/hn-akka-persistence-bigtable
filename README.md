# Hafslund.Akka.Persistence.Bigtable

## Installation
This plugin is (supposed to) be released publicly to nuget.org, and the latest released version can be installed by:
```
dotnet add package Hafslund.Akka.Persistence.Bigtable
```

To use the plugin as an extension, add the following line of code to the Akka system initialization:
```
BigtablePersistence.Get(actorSystem);
```
(Note that this is similar to how the Cluster extension is set up, e.g. `Cluster.Get(actorSystem)`).


## Setup

The extension will load default configuration (see `reference.conf` from plugin source code), but as a minimum, the tablename has to be specified. There is no option to feed in a connection string for the table, as this is not how the Google Cloud SDK is set up to authenticate, see 

To activate the journal plugin, add the following lines to your actor system configuration file:
```
akka.persistence.journal.plugin = "akka.persistence.journal.bigtable"
akka.persistence.journal.table-name = "your/bigtable/table/reference"
```

Similar configuration may be used to setup a Bigtable snapshot store:
```
akka.persistence.snapshot.plugin = "akka.persistence.snapshot.bigtable"
akka.persistence.snapshot.table-name = "your/bigtable/table/reference"
```

Note that the tables with column family must be created before running this plugin, as there is no `auto-initialize` configuration setting provided (this option would require a different authorization level with create access to Bigtable, than what it is recommended that the akka system should run under). For a brief introduction on how to create tables and column families, see https://cloud.google.com/bigtable/docs/quickstart-cbt.

For a working CBT installation setup with a ~/.cbtrc file pointing to the right project and instance, the following set of commands gives an example:
```
cbt createtable MyJournalEvents
cbt createfamily MyJournalEvents f
cbt createtable MySnapshotStore
cbt createfamily MySnapshotStore f
```

Note that the family name `f` is, in addition to being short and sweet, hard coded in the plugin. If the table is not created with a column family named `f`, nothing will be persisted.

### Configuration

Both journal and snapshot store share the same configuration keys (however they resides in separate scopes, so they are definied distinctly for either journal or snapshot store):

```hocon
akka.persistence {
  journal {
    bigtable {
	    # qualified type name of the Google Bigtable persistence journal actor
	    class = "Hafslund.Akka.Persistence.Bigtable.Journal.BigtableJournal, Hafslund.Akka.Persistence.Bigtable"

	    # the name of the Google Bigtable used to persist journal events
	    # (full name, i.e. 'projects/<project-id>/instances/<instance-id>/tables/<table-name> )
	    table-name = ""

	    # the name of the table used to persist sharding journal events. 
	    # If left empty/unconfigured, it will use the table specified under `table-name`
	    sharding-table-name = ""

	    # dispatcher used to drive journal actor
	    plugin-dispatcher = "akka.actor.default-dispatcher"
    }
  }  

  snapshot-store {
    bigtable {
	    # qualified type name of the Google Bigtable persistence snapshot storage actor
	    class = "Hafslund.Akka.Persistence.Bigtable.Snapshot.BigtableSnapshotStore, Hafslund.Akka.Persistence.Bigtable"

	    # the name of the Google Bigtable used to persist snapshots
	    # (full name, i.e. 'projects/<project-id>/instances/<instance-id>/tables/<table-name> )
	    table-name = ""

	    # the name of the table used to persist sharding snapshots. 
	    # If left empty/unconfigured, it will use the table specified under `table-name`
	    sharding-table-name = ""

	    # dispatcher used to drive snapshot storage actor
	    plugin-dispatcher = "akka.actor.default-dispatcher"
    }
  }
```