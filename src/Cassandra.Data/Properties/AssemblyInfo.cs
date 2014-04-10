using System.Reflection;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;

// See the SharedAssemblyInfo.cs file for information shared by all assemblies
[assembly: AssemblyTitle("Cassandra.Data")]
[assembly: AssemblyDescription("DataStax ADO.NET Driver for Apache Cassandra")]

// The following GUID is for the ID of the typelib if this project is exposed to COM
[assembly: Guid("0da6f43a-d752-49b2-a0cf-e2f94d505b75")]
// Make internals visible to the Tests project(s)
[assembly: InternalsVisibleTo("Cassandra.IntegrationTests")]
[assembly: InternalsVisibleTo("Cassandra.Tests")]