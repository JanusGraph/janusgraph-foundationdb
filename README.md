# FoundationDB Storage Adapter for JanusGraph

[JanusGraph](http://janusgraph.org) is an [Apache TinkerPop](http://tinkerpop.apache.org) enabled graph database that supports a variety of storage and indexing backends. This project adds [FoundationDB](http://foundationdb.org) to the supported list of backends. FoundationDB is a distributed, ACID key-value store.

# Features

JanusGraph, coupled with the FoundationDB storage adapter provides the following unique features:

* High availability
* ACID transactions

# Limitations

# Compatibility Matrix

|FDB Storage Adapter|JanusGraph|FoundationDB|
|-:|-:|-:|
|0.1.0|0.3.0|5.2.5|

# Getting started

## Installing from a binary release
Binary releases can be found on [GitHub](http://github.com/experoinc/janusgraph-foundationdb/releases).

1. Download the JanusGraph [release](https://github.com/JanusGraph/janusgraph/releases) that is compatible up with the FoundationDB storage adapter.
2. Download the desired FoundationDB storage adapter release.
3. Unzip the storage adapter zip and copy the contents of the `lib` directory into your JanusGraph `ext` directory.
4. Copy the `janusgraph-foundationdb.properties` file into your JanusGraph `conf` directory.

## Installing from source

1. Clone the repository from GitHub.
    `git clone http://github.com/experoinc/janusgraph-foundationdb`
# Configuration Options

|Property|Description|Default|
|-|-|-|
|`storage.fdb.directory`|Name of the JanusGraph storage directory in FoundationDB.|`janusgraph`|
|`storage.fdb.version`|The FoundationDB client version.|`5.2.0`|
|`storage.fdb.cluster_file_path`|The location of the `fdb.cluster` file.|`/etc/foundationdb/fdb.cluster`|
|`storage.fdb.serializable`|JanusGraph transactions are serializable if `true`, read committed if `false`.|`true`|