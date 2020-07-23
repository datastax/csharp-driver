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

using System;
using System.Collections.Generic;

using Cassandra.DataStax.Graph;
using Cassandra.SessionManagement;

namespace Cassandra.Requests
{
    internal interface IGraphProtocolResolver
    {
        /// <summary>
        /// Resolves the graph protocol version according to the keyspace metadata or language.
        /// See <see cref="GraphOptions.GraphProtocolVersion"/> for an explanation.
        /// </summary>
        GraphProtocol GetDefaultGraphProtocol(IInternalSession session, IGraphStatement statement, GraphOptions options);

        /// <summary>
        /// Gets the row parser for graph result sets according to the protocol version.
        /// </summary>
        Func<Row, GraphNode> GetGraphRowParser(GraphProtocol graphProtocolVersion);

        /// <summary>
        /// Gets the parameters serializer according to the protocol version.
        /// </summary>
        Func<IDictionary<string, object>, string> GetParametersSerializer(GraphProtocol graphProtocolVersion);
    }
}