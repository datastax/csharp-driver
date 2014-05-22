using System.Reflection;
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