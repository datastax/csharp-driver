Linq2CQL Driver
===========

The core module of the Datastax C# Linq Driver for Apache Cassandra (C*). This
module offers a Linq driver and simple ORM mapper to work with
CQL3. 

Features
--------

The features provided by this Linq2CQL driver includes:


Prerequisite
------------

This driver depends on core driver (Cassandra.dll)

Installing
----------

This driver has not been released yet and will need to be compiled manually.

Getting Started
---------------

Suppose you have a Cassandra cluster running on 3 nodes whose hostnames are:
cass1, cass2 and cass3. A simple example using this core driver could be::

    var cluster = Cluster.Builder()
                        .AddContactPoints("cass1", "cass2")
                        .Build();
    var session = cluster.Connect("db1");

	var context = new SampleContext(session);

	var table = context.GetTable<SamplEnt>();

    foreach (var ent in (from e in table select e).Execute())
        // do something ...

