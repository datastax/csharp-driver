Driver Core
===========

The core module of the Datastax C# Driver for Apache Cassandra (C*). This
module offers a simple (as in, not abstracted) but complete API to work with
CQL3. The main goal of this module is to handle all the functionality related
to managing connections to a Cassandra cluster (but leaving higher level
abstraction like object mapping to separate modules).


Features
--------

The features provided by this core module includes:
  - Asynchronous: the driver uses the new CQL binary protocol asynchronous
    capabilities. Only a relatively low number of connection per nodes needs to
    be maintained open to achieve good performance.
  - Nodes discovery: the driver automatically discover and use all nodes of the
    C* cluster, including newly bootstrapped ones.
  - Configurable load balancing: the driver allow for custom routing/load
    balancing of queries to C* nodes. Out of the box, round robin is provided
    with optional data-center awareness (only nodes from the local data-center
    are queried (and have connections maintained to)) and optional token
    awareness (i.e the ability to prefer a replica for the query as coordinator).
  - Transparent fail-over. If C* nodes fail (are not reachable), the driver
    automatically and transparently tries other nodes and schedule
    reconnection to the dead nodes in the background.
  - C* tracing handling. Tracing can be set on a per-query basis and the driver
    provides a convenient API to retrieve the trace.
  - Convenient schema access. The driver exposes the C* schema in a usable way.
  - Configurable retry policy. A retry policy can be set to define a precise
    comportment to adopt on query execution exceptions (timeouts, unavailable).
    This avoids having to litter client code with retry related code.
  - Performance counters which lets to monitor performance of the driver and Cassandra.

Prerequisite
------------

This driver uses the binary protocol that will be introduced in C* 1.2.
It will thus only work with a version of C* >= 1.2. Since at the time of this
writing C* 1.2 hasn't been released yet, at least the beta2 release needs to be
used (the beta1 is known to *not* work with this driver). Furthermore, the
binary protocol server is not started with the default configuration file
coming with Cassandra 1.2. In the cassandra.yaml file, you need to set::

    start_native_transport: true

Installing
----------

This driver has not been released yet and will need to be compiled manually.

Getting Started
---------------

Suppose you have a Cassandra cluster running on 3 nodes whose hostnames are:
cass1, cass2 and cass3. A simple example using this core driver could be::

    Cluster cluster = Cluster.Builder()
                        .AddContactPoints("cass1", "cass2")
                        .Build();
    Session session = cluster.Connect("db1");

    foreach(var row in session.Execute("SELECT * FROM table1"))
        // do something ...


Please note that when we build the Cluster object, we only provide the address
to 2 Cassandra hosts. We could have provided only one host or the 3 of them,
this doesn't matter as long as the driver is able to contact one of the host
provided as "contact points". Even if only one host was provided, the driver
would use this host to discover the other ones and use the whole cluster
automatically. This is also true for new nodes joining the cluster.


Performance Counters
--------------------

To enable performance counters "Debugging.PerformanceCountersEnabled" property have to be set to true (it's set to false by default).
Data collected by counters can be reached from Performance Monitor.
To launch Performance Monitor(in Windows7): 
	
	Control Panel => Performance Information and Tools => Advanced Tools => Open Performance Monitor

In Performance Monitor click "Add" button. From the list of counter categories select and expand "DataStax Cassandra C# driver".
Add counters that you want to monitor.