# Terminology
In multi-zone operation a single logical HopsFS cluster is divided into two physical cluster, each with its own MySQL Cluster cluster, namenodes and datanodes.
Due to the way conflict resolution works in MySQL Cluster using asynchronous replication, one of the metadata storage clusters will be designated as **primary** and the other as **secondary**.
Transactions committed on the primary cluster will never be rolled back, while transactions on the secondary clusters can be rolled back if they are in conflict with transactions originating in the primary cluster.
For convenience, in this document, we will refer to **primary hdfs cluster** when talking about namenodes and datanodes located in the same cluster as the primary metadata store, and **secondary hdfs cluster** when referring to the other nodes.
In this context we also refer to the **local metadata store** as the MySQL Cluster local to the cluster being discussed (which may either be primary or secondary).
During multi-zone operations, namenodes in the primary hdfs clusters will work as before, holding only connections to the local metadata store, while namenodes in the secondary hdfs cluster will hold both a **LOCAL** connection to the local metadata store and a **PRIMARY** connection to the primary metadata store.

# Configuration
The `hdfs-site.xml` file is used to configure HopsFS for multi-zone operations, while `ndb-config.properties` is used to specify connection parameters to the metadata clusters.
To enable multi-zone mode, the `io.hops.metadata.multizone` flag must be set to `"true"`.
The `io.hops.metadata.zone` must be set to `primary` or `secondary` depending on the role of the local metadata store.

Configuration for the **primary** cluster is then the same as the single-zone configuration with `io.hops.metadata.local.clusterj.*` and `io.hops.metadata.local.mysqlserver.*` keys.
The keys for clusterj follow the same format expected by the [clusterj driver](https://dev.mysql.com/doc/ndbapi/en/mccj-using-clusterj-start.html): `com.mysql.clusterj.*`.
Exact keys can be found in [com.mysql.clusterj.Constants](https://dev.mysql.com/doc/ndbapi/en/mccj-clusterj-constants.html#mccj-clusterj-constants-default_property_connection_pool_size).

Configuration for the SECONDARY cluster is performed by using the `io.hops.metadata.local.{clusterj, mysqlserver}.*` keys to define the connection to the local metadata store and the `io.hops.metadata.primary.{clusterj, mysqlserver}.*` keys to define the connection to the remote (primary) metadata store.

## Reconnection
Note that for the reconnection feature to work properly, the connection timeout should be lowered:
```ini
io.hops.metadata.(local|remote).clusterj.connect.timeout.mgm = 2000
io.hops.metadata.(local|remote).clusterj.connect.retries = 0
io.hops.metadata.(local|remote).clusterj.connect.delay = 0
```

## Example:
Primary configuration

```xml
<!-- hdfs-site.xml -->
<configuration>
    <property>
        <name>io.hops.metadata.multizone</name>
        <value>true</value>
    </property>

    <property>
        <name>io.hops.metadata.zone</name>
        <value>primary</value>
    </property>
...
```

```ini
# ndb-config.ini
io.hops.metadata.local.clusterj.connectstring=meta.cluster1.example.com
io.hops.metadata.local.clusterj.database=hopsfs_meta

io.hops.metadata.local.mysql.host = meta.cluster1.example.com
io.hops.metadata.local.mysql.port = 3306
io.hops.metadata.local.mysql.username = ...
io.hops.metadata.local.mysql.password = ...
...
```

Secondary example configuration:

```xml
<!-- hdfs-site.xml -->
<configuration>
    <property>
        <name>io.hops.metadata.multizone</name>
        <value>true</value>
    </property>

    <property>
        <name>io.hops.metadata.zone</name>
        <value>secondary</value>
    </property>
...
```

```ini
# connection to secondary (local)
io.hops.metadata.local.clusterj.connectstring=meta.cluster2.example.com
io.hops.metadata.local.clusterj.database=hopsfs_meta
io.hops.metadata.local.mysql.host = meta.cluster2.example.com
io.hops.metadata.local.mysql.port = ...
io.hops.metadata.local.mysql.username = ...
io.hops.metadata.local.mysql.password = ...
...

# connection to primary
io.hops.metadata.primary.clusterj.connectstring=meta.cluster1.example.com
io.hops.metadata.primary.clusterj.database=hopsfs_meta
io.hops.metadata.primary.mysql.host = meta.cluster1.example.com
io.hops.metadata.primary.mysql.port = ...
io.hops.metadata.primary.mysql.username = ...
io.hops.metadata.primary.mysql.password = ...
...
```
