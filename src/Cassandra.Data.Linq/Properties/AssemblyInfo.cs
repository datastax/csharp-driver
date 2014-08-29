//
//      Copyright (C) 2012-2014 DataStax Inc.
//
//   Licensed under the Apache License, Version 2.0 (the "License");
//   you may not use this file except in compliance with the License.
//   You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
//   Unless required by applicable law or agreed to in writing, software
//   distributed under the License is distributed on an "AS IS" BASIS,
//   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//   See the License for the specific language governing permissions and
//   limitations under the License.
//

﻿using System.Reflection;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;

// See the SharedAssemblyInfo.cs file for information shared by all assemblies
[assembly: AssemblyTitle("Cassandra.Data.Linq")]
[assembly: AssemblyDescription("DataStax .NET LINQ Driver for Apache Cassandra")]

// The following GUID is for the ID of the typelib if this project is exposed to COM
[assembly: Guid("d1aad658-1aca-459c-9695-a1930131bafa")]
// Make internals visible to the Tests project(s)
[assembly: InternalsVisibleTo("Cassandra.IntegrationTests")]
[assembly: InternalsVisibleTo("Cassandra.Tests")]
[assembly: InternalsVisibleTo("Cassandra.Data.EntityContext")]