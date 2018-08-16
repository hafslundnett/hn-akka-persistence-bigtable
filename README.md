# Hafslund.Akka.Persistence.Bigtable

## Installation
This plugin is (supposed to) be released publicly to nuget.org, and the latest released version can be installed by:
```
dotnet add package Hafslund.Akka.Persistence.Bigtable
```

## Setup

To activate the journal plugin, add the following lines to your actor system configuration file:
```
akka.persistence.journal.plugin = "akka.persistence.journal.bigtable"
akka.persistence.journal.table-name = "your/bigtable/table/reference"
```

Similar configuration may be used to setup a MongoDB snapshot store:
```
akka.persistence.snapshot.plugin = "akka.persistence.snapshot.bigtable"
akka.persistence.snapshot.table-name = "your/bigtable/table/reference"
```

### Configuration

Both journal and snapshot store share the same configuration keys (however they resides in separate scopes, so they are definied distinctly for either journal or snapshot store):

```hocon
akka.persistence {
  journal {
    bigtable {
      # qualified type name of the Google Bigtable persistence journal actor
      class = "Hafslund.Akka.Persistence.Bigtable.Journal.BigtableJournal, Hafslund.Akka.Persistence.Bigtable"

	    # the name of the Google Bigtable used to persist journal events
	    table-name = ""

	    # the name of the table used to persist sharding journal events. If left empty/unconfigured, it will use the table specified under `table-name`
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
	    table-name = ""

	    # the name of the table used to persist sharding snapshots. If left empty/unconfigured, it will use the table specified under `table-name`
	    sharding-table-name = ""

	    # dispatcher used to drive snapshot storage actor
      plugin-dispatcher = "akka.actor.default-dispatcher"
    }
  }
```