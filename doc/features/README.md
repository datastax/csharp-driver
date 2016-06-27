*This is the documentation for the DataStax Enterprise C# Driver for [DSE][dse]. This driver is built on top of the
[DataStax C# driver for Apache Cassandra][core-driver] and enhanced for the adaptive data management and mixed
workload capabilities provided by [DataStax Enterprise][dse]. Therefore a lot of the underlying concepts are the same
and to keep this documentation focused we will be linking to the relevant sections of the [DataStax C# driver
for Apache Cassandra][core-driver-docs] documentation where necessary.*

# Features

The DataStax Enterprise C# Driver is feature-rich and highly tunable C# client library for [DataStax Enterprise][dse].

## Usage

- [Address resolution <sup>1</sup>](http://datastax.github.io/csharp-driver/features/address-resolution/)
- [CQL data types to C# types <sup>1</sup>](http://datastax.github.io/csharp-driver/features/datatypes/)
- [Components <sup>1</sup>](http://datastax.github.io/csharp-driver/features/components/)
    - [Core](http://datastax.github.io/csharp-driver/features/components/core/)
    - [Linq](http://datastax.github.io/csharp-driver/features/components/linq/)
    - [Mapper](http://datastax.github.io/csharp-driver/features/components/mapper/)
    - [ADO.NET](http://datastax.github.io/csharp-driver/features/components/adonet/)
- [Connection hearbeat <sup>1</sup>](http://datastax.github.io/csharp-driver/features/connection-heartbeat/)
- [Connection pooling <sup>1</sup>](http://datastax.github.io/csharp-driver/features/connection-pooling/)
- [Geospatial types support](geotypes)
- [Graph support](graph-support)
- [Query warnings <sup>1</sup>](http://datastax.github.io/csharp-driver/features/query-warnings/)
- [Result paging <sup>1</sup>](http://datastax.github.io/csharp-driver/features/paging/)
- [Parametrized queries <sup>1</sup>](http://datastax.github.io/csharp-driver/features/parametrized-queries/)
- [Routing queries <sup>1</sup>](http://datastax.github.io/csharp-driver/features/routing-queries/)
- [Speculative executions <sup>1</sup>](http://datastax.github.io/csharp-driver/features/speculative-retries/)
- [Tuning policies <sup>1</sup>](http://datastax.github.io/csharp-driver/features/tuning-policies/)
- [User-defined functions and aggregates <sup>1</sup>](http://datastax.github.io/csharp-driver/features/udfs/)
- [User-defined types <sup>1</sup>](http://datastax.github.io/csharp-driver/features/udts/)

---

1. Documentation hosted on the DataStax C# driver for Apache Cassandra website.

[dse]: http://www.datastax.com/products/datastax-enterprise
[core-driver]: https://github.com/datastax/csharp-driver-dse
[core-driver-docs]: http://datastax.github.io/csharp-driver/
[core-features]: http://datastax.github.io/csharp-driver/features/
