//
//       Copyright (C) DataStax Inc.
//
//    Licensed under the Apache License, Version 2.0 (the "License");
//    you may not use this file except in compliance with the License.
//    You may obtain a copy of the License at
//
//       http://www.apache.org/licenses/LICENSE-2.0
//
//    Unless required by applicable law or agreed to in writing, software
//    distributed under the License is distributed on an "AS IS" BASIS,
//    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//    See the License for the specific language governing permissions and
//    limitations under the License.
//

using System;
using System.Linq;
using Cassandra.IntegrationTests.SimulacronAPI.Models.Logs;

namespace Cassandra.IntegrationTests.SimulacronAPI.Models
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