using System.Reflection;

namespace Cassandra.OpenTelemetry
{
    public static class CassandraInstrumentation
    {
        internal static readonly AssemblyName AssemblyName = typeof(CassandraInstrumentation).Assembly.GetName();
        internal static readonly string Version = AssemblyName.Version.ToString();
        public static readonly string ActivitySourceName = AssemblyName.Name;
    }
}
