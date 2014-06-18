# Cassandra Entity Context

Similar to [ObjectContext][2], this is an implementation of the Entity Framework for Apache Cassandra.

This package is included to ease the migration from the version 1.0 of the DataStax C# driver, 
but using the Entity Framework-like pattern with Apache Cassandra is **discouraged** as it can lead to major performance issues
and unexpected behavior. 

To take advantage of the latest Cassandra features and benefit of best performance, use [the core and Linq modules of 
the C# driver for Apache Cassandra][1].

[1]: https://github.com/datastax/csharp-driver
[2]: http://msdn.microsoft.com/en-us/library/system.data.objects.objectcontext(v=vs.110).aspx