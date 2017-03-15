# ChangeLog - DataStax Enterprise C# Driver

## 2.0.0-beta1

2017-03-15

### Notable Changes

- DSE 5.1 Support
- _Breaking:_ The DSE driver now contains the core driver instead of depending on it. The driver package exposes
a single root namespace `Dse`. Classes that used to be under `Cassandra` namespace are now exposed in the `Dse`
namespace, you should change your `using` directive to point to `Dse` instead.

### Features

- [CSHARP-529] - Make protocol negotiation more resilient
- [CSHARP-533] - Duration type support
- [CSHARP-535] - DSE Auth 5.1: Support Proxy Authentication in 5.1
- [CSHARP-536] - Support DSE 5.1 DateRangeField
- [CSHARP-537] - Read optional workload set from node metadata
- [CSHARP-541] - Merge Cassandra driver code base into DSE driver

### Bug Fixes

- [CSHARP-540] - Table metadata can not read custom types column info

## 1.1.1

2017-02-15

### Features

- [CSHARP-521] - Update core driver dependency to v3.2.1

## 1.1.0

2016-12-20

### Features

- [CSHARP-488] - .NET Core Support for the DSE driver
- [CSHARP-514] - Expose query and parameters as SimpleGraphStatement properties
- [CSHARP-521] - Update core driver dependency to v3.2.0

## 1.0.1

2016-09-29

### Features

- [CSHARP-491] - Support Newtonsoft.Json version 9
- [CSHARP-502] - Update core driver dependency to v3.0.9
