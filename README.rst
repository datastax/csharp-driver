Datastax C# Driver for Apache Cassandra
=======================================

A C# client driver for Apache Cassandra. This driver works exclusively with
the Cassandra Query Language version 3 (CQL3) and Cassandra's binary protocol.

- NuGet: https://nuget.org/packages/CassandraCSharpDriver/
- JIRA: https://datastax-oss.atlassian.net/browse/CSHARP
- MAILING LIST: https://groups.google.com/a/lists.datastax.com/forum/#!forum/csharp-driver-user
- IRC: #datastax-drivers on irc.freenode.net
- DOCS: http://www.datastax.com/documentation/developer/csharp-driver/1.0/webhelp/index.html
- API: http://www.datastax.com/drivers/csharp/apidocs/

The driver architecture is based on layers. At the bottom lies the driver core,
located in Cassandra.dll Assembly. This core handles everything related to the 
connections to the Cassandra cluster (connection pool, discovering new nodes, ...) 
and exposes a simple, relatively low-level, API. 
Linq2CQL is built on top of driver core. It is located in Cassandra.Data.Linq.dll 
Assembly. Linq2CQL is a linq driver for Cassandra.

The driver contains the following modules:

- Cassandra: the core layer.
- Cassandra.Data.Linq: Linq2CQL driver
- TestRunner: basic unit-test environment 
- Cassandra.Test: unit-tests for the core driver
- Cassandra.Data.Linq.Test: unit-tests for Linq2CQL driver
- Playground: simple app that presents the basic usage of Linq2CQL driver
 
Please refer to the README of each module for more information.

License
-------
Copyright 2013, DataStax

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
