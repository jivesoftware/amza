![Amza](https://github.com/jivesoftware/amza/wiki/images/amza-logo.png)
=========

#### What It Is
 Amza is a high-availability distributed log with the ability to overlay a lexicographically order prefix + key value store which offers the same consistency constraints as Hbase (but HA) or Cassandra. Amza offers additional traits simliar to those found in RethinkDB, Kafka, and Zookeeper.

#### Use Case
* Standalone Amza
    * Use it in place of HBase
    * Use it in place of Cassandra
    * Use it in place of Kafka
    * Use it in place of Zookeeper
    
* Embedded Amza
    * Your service only needs to read and write its state locally, but if/when an instance is lost and irrecoverable you want to bring it back online on a different node with the same state.
    * Your service wants to share a copy of its state with other services and eventually consistent replication is acceptable.
    * Your collection of services always wants to have an elected leader for one or more topics.
    * Guaranteed delivery of messaging (pub-sub)

#### How It Works
 At its core, Amza is any number of total-ordered, distributed, replicated logs. Replication of log entries is done using an epidemic (gossip) protocol. Each log has an intrinsic elected [leader](https://github.com/jivesoftware/aquarium) which can be leveraged or ignored depending on the use case or desired data consistency.  One of several different implementations of an in-memory or persistent index can be overlaid on top of a given log to enable different use cases.
* For example:
    * Using intrinsic leader election with persistent indexes and immediate consistency constraints, Amza can be used like HBase.
    * Using persistent indexes and eventual consistency constraints, Amza can be used like Cassandra.
    * Using intrinsic leader election with in-memory indexes and immediate consistency constraints, Amza can be used to replace typical Kafka uses cases.
    * Using intrinsic leader election with in-memory indexes and immediate consistency constraints, Amza can be used to replace typical Zookeeper uses cases.
  
 
#### Getting Started
Check out Amza over at [the wiki](https://github.com/jivesoftware/amza/wiki).

#### Licensing
Amza is licensed under the Apache License, Version 2.0. See LICENSE for full license text.
