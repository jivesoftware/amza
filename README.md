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
    
* Emdbbeded Amza
    * Your service only needs to read and write its state locally but if / when said instance is irrecoverable lost you need to bring it back online on a different node with the same state. 
    * Your services wants to share a copy of its state with other services and eventually consistent replication is ok.
    * Your collection of services always wants to have an elected leader.
    * Guaranteed delivery of messaging (Pub-sub) 

#### How It Works
 At its core Amza is N totally order distributed replicated logs. Replication of log entries is done using an epidemic protocol. Each log intrisically has an elected [leader](https://github.com/jivesoftware/aquarium) which can be leveraged or ignored depending on the use case or desired data consisteny.  One of several different implementations of an in memory or on disk indexe can be overlayed on top of a given log to enable different use cases. 
* For example:
   * Using the intrinsic leader election with disk backed indexes and immediate consistency constraints Amza can be used like Hbase. 
  * Using disk backed indexes and eventaully consistency constraints Amza can be used like Cassandra.
  * Using the intrinsic leader election with in memory indexes and immediate consistency constraints Amza can be used to replace typical kafka uses cases.
  * Using the intrinsic leader election with in memory indexes and immediate consistency constraints Amza can be used to replace typical zookeeper uses cases. 
  
 
#### Getting Started
Check out Amza over at [the wiki](https://github.com/jivesoftware/amza/wiki).

#### Licensing
Amza is licensed under the Apache License, Version 2.0. See LICENSE for full license text.
