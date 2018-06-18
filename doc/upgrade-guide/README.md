# Upgrade Guide

The purpose of this guide is to detail the changes made by the successive versions of the DataStax C# Driver that 
are relevant to for an upgrade from prior versions.

If you have any question or comment, you can [post it on the mailing list][mailing-list].

## 2.3

The `DowngradingConsistencyRetryPolicy` is now deprecated. It will be removed in the following major version of the 
driver.

The main motivation is the agreement that this policy's behavior should be the application's concern, not the driver's.

We recognize that there are use cases where downgrading is good – for instance, a dashboard application would present
the latest information by reading at `QUORUM`, but it's acceptable for it to display stale information by reading at
ONE sometimes.

But APIs provided by the driver should instead encourage idiomatic use of a distributed system like Apache Cassandra,
and a downgrading policy works against this. It suggests that an anti-pattern such as "try to read at `QUORUM`, but
fall back to `ONE` if that fails" is a good idea in general use cases, when in reality it provides no better consistency
guarantees than working directly at `ONE`, but with higher latencies.

We therefore urge users to carefully choose upfront the consistency level that works best for their use cases, and
should they decide that the downgrading behavior of `DowngradingConsistencyRetryPolicy` remains a good fit for certain
use cases, they will now have to implement this logic themselves, either at application level, or alternatively at
driver level, by rolling out their own downgrading retry policy.

[mailing-list]: https://groups.google.com/a/lists.datastax.com/forum/#!forum/csharp-driver-user