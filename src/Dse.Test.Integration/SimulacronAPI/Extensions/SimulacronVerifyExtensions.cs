using System;
using System.Linq;
using Dse.Test.Integration.SimulacronAPI.Models.Logs;

namespace Dse.Test.Integration.SimulacronAPI.Models
{
    public static class SimulacronVerifyExtensions
    {
        public static bool AnyQuery(this SimulacronClusterLogs logs, Func<RequestLog, bool> func)
        {
            return logs.DataCenters.Any(dc => dc.Nodes.Any(node => node.Queries.Any(func)));
        }

        public static bool HasQueryBeenExecuted(this SimulacronClusterLogs logs, string query)
        {
            return logs.AnyQuery(q => q.Query == query);
        }
    }
}