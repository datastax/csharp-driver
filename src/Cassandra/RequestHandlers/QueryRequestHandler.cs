//
//      Copyright (C) 2012 DataStax Inc.
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

using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Text;

namespace Cassandra.RequestHandlers
{
    /// <summary>
    /// Sends a query request and parses the response.
    /// </summary>
    internal class QueryRequestHandler : ExecuteQueryRequestHandler
    {
        override public void Begin(Session owner, int streamId)
        {
            Connection.BeginQuery(streamId, CqlQuery, owner.RequestCallback, this, owner, IsTracing, QueryProtocolOptions.CreateFromQuery(Statement, owner.Cluster.Configuration.QueryOptions.GetConsistencyLevel()), Consistency);
        }

        override public void Process(Session owner, IAsyncResult ar, out object value)
        {
            value = ProcessResponse(Connection.EndQuery(ar, owner), owner);
        }
    }
}
