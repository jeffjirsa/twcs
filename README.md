

TWCS: Time Window Compaction Strategy
==================================


Motivation
----------

TWCS is a proposed improvement on DTCS. The motivations for TWCS are explained at:

https://issues.apache.org/jira/browse/CASSANDRA-9666

And:

http://www.slideshare.net/JeffJirsa1/cassandra-summit-2015-real-world-dtcs-for-operators

Setup
-----

Checkout the repo:

```
git clone https://github.com/jeffjirsa/twcs/
```

Switch to the right version (for newest 2.1 release, for example):

```
git checkout origin cassandra-2.1 
```

Build the jar:

```
cd TimeWindowCompactionStrategy
mvn compile
mvn package
```

The resulting jar will be placed in target/, copy it to the classpath of your cassandra server.

Enabling
----------

Use the `WITH compaction=` directive to set the compaction strategy.  By default, TWCS creates 1 Day buckets and assumes MICROSECOND resolution.

Use `compaction_window_unit` of MINUTES, HOURS, or DAYS and `compaction_window_size` as an integer to group sstable into buckets of a fixed size that matches your use case.  
Use `timestamp_resolution` to let the compaction strategy know which time unit to use.


    CREATE TABLE twcs (
    id int,
    value int,
    PRIMARY KEY ((id))
    ) WITH compaction= {
        'compaction_window_unit': 'MINUTES',
        'compaction_window_size': '3',
        'class':'com.jeffjirsa.cassandra.db.compaction.TimeWindowCompactionStrategy'
        };


Source
------

This repository ( https://github.com/jeffjirsa/twcs ) will be used for standalone TWCS code (simple jar to drop into lib/). For practical purposes, in this repository the compaction strategy is `com.jeffjirsa.cassandra.db.compaction.TimeWindowCompactionStrategy`

TWCS is also available as a C* fork (in the hopes that patches are accepted upstream): 

2.1: https://github.com/jeffjirsa/cassandra/tree/twcs-2.1

2.2: https://github.com/jeffjirsa/cassandra/tree/twcs-2.2

Trunk / 3.0: https://github.com/jeffjirsa/cassandra/tree/twcs

In the full fork, the compaction strategy is: `org.apache.cassandra.db.compaction.TimeWindowCompactionStrategy`

License
-------

TWCS is provided under the Apache License, V2: http://www.apache.org/licenses/LICENSE-2.0
