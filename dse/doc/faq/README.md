# Frequently Asked Questions

### Which versions of DSE does the driver support?

The driver supports versions from 4.8 to 5 of [DataStax Enterprise][dse].

### How can I upgrade from the Cassandra driver to the DSE driver?

There is a section in the [Getting Started](../getting-started/) page.

### Where can I find more tutorials or documentation?

All the functionality present in the Cassandra driver is available on the DSE driver, [any tutorial or documentation
that references the DataStax C# driver for Apache Cassandra also applies to this driver][core-features].

### Can I use a single `IDseCluster` and `IDseSession` instance for graph and CQL?

It's currently not recommended, as different different workloads should be distributed across different datacenters
and the load balancing policy should select the appropriate coordinator for each workload.
We are planning to introduce execution profiles, that will allow you to use the same `IDseSession` instance
for all workloads.

### Should I dispose or shut down `IDseCluster` or `IDseSession` instances after executing a query?

No, only call `cluster.Shutdown()` once in your application's lifetime, normally when you shutdown your application.

[dse]: http://www.datastax.com/products/datastax-enterprise
[core-features]: http://datastax.github.io/csharp-driver/features/