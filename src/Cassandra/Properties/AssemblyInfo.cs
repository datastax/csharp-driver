using System.Reflection;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;

// See the SharedAssemblyInfo.cs file for information shared by all assemblies
[assembly: AssemblyTitle("Cassandra")]
[assembly: AssemblyDescription("Datastax .NET Driver for Apache Cassandra")]

// The following GUID is for the ID of the typelib if this project is exposed to COM
[assembly: Guid("08e7f507-4f8f-4136-92d0-b316b5266afa")]

[assembly: InternalsVisibleTo("Cassandra.Data.Linq")]
// Make internals visible to the Tests project(s)
[assembly: InternalsVisibleTo("Cassandra.IntegrationTests")]
[assembly: InternalsVisibleTo("Cassandra.Tests")]