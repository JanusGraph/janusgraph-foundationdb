# FoundationDB Storage Adapter for JanusGraph

[JanusGraph](http://janusgraph.org) is an [Apache TinkerPop](http://tinkerpop.apache.org) enabled graph database that supports a variety of storage and indexing backends. This project adds [FoundationDB](http://foundationdb.org) to the supported list of backends. FoundationDB is a distributed, ACID key-value store.

# Features

JanusGraph, coupled with the FoundationDB storage adapter provides the following unique features:

* ACID transactions

# Limitations

# Compatibility Matrix

|FDB Storage Adapter|JanusGraph|FoundationDB|
|------------------:|---------:|-----------:|
|0.1.0              |     0.3.0|       5.2.5|

# Getting started from a release

Binary releases can be found on [GitHub](http://github.com/experoinc/janusgraph-foundationdb/releases).

# Getting started from source

1. Clone the repository from GitHub.
    `git clone http://github.com/experoinc/janusgraph-foundationdb`

# Configuration Options

|Property|Description|Default|
|-|-|-|
|`storage.fdb.directory`|Name of the JanusGraph storage directory in FoundationDB.|`janusgraph`|
|`storage.fdb.version`|The FoundationDB client version.|`5.2.0`|
|`storage.fdb.cluster_file_path`|The location of the `fdb.cluster` file.|`/etc/foundationdb/fdb.cluster`|
|`storage.fdb.serializable`|JanusGraph transactions are serializable if `true`, read committed if `false`.|`true`|