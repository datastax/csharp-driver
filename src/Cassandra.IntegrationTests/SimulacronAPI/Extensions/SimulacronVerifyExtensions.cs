using System;
using System.Linq;

namespace Cassandra.IntegrationTests.SimulacronAPI.Models
{
    public static class SimulacronVerifyExtensions
    {
        public static bool AnyQuery(this SimulacronLogs logs, Func<SimulacronLogsQuery, bool> func)
        {
            return logs.DataCenters.Any(dc => dc.Nodes.Any(node => node.Queries.Any(func)));
        }

        public static bool HasQueryBeenExecuted(this SimulacronLogs logs, string query)
        {
            return logs.AnyQuery(q => q.Query == query);
        }
    }
}