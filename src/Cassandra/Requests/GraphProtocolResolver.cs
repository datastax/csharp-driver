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
using Cassandra.DataStax.Graph;

namespace Cassandra.Requests
{
    internal class GraphProtocolResolver : IGraphProtocolResolver
    {
        private const string CoreEngine = "Core";

        private static readonly Logger Logger = new Logger(typeof(GraphProtocolResolver));

        /// <inheritdoc />
        public GraphProtocol GetDefaultGraphProtocol(ISession session, IGraphStatement statement, GraphOptions options)
        {
            var protocol = GetByKeyspaceMetadata(session, statement, options);

            if (protocol != null)
            {
                GraphProtocolResolver.Logger.Verbose(
                    "Resolved graph protocol {0} according to the keyspace metadata.", protocol.Value.GetInternalRepresentation());
                return protocol.Value;
            }

            protocol = GetByLanguage(statement, options);

            if (protocol != null)
            {
                GraphProtocolResolver.Logger.Verbose(
                    "Resolved graph protocol {0} according to the graph language option.", protocol.Value.GetInternalRepresentation());
                return protocol.Value;
            }

            protocol = GraphProtocol.GraphSON2;

            GraphProtocolResolver.Logger.Verbose(
                "Unable to resolve graph protocol according to keyspace metadata or graph language. " +
                "Resolved graph protocol to {0}.", protocol.Value.GetInternalRepresentation());
            return protocol.Value;
        }

        private GraphProtocol? GetByKeyspaceMetadata(
            ISession session, IGraphStatement statement, GraphOptions options)
        {
            var graphName = statement.GraphName ?? options.Name;

            if (graphName == null)
            {
                return null;
            }

            var ksMetadata = session.Cluster.Metadata.GetKeyspaceFromCache(graphName);

            if (ksMetadata?.GraphEngine == null)
            {
                return null;
            }

            if (ksMetadata.GraphEngine.Equals(
                GraphProtocolResolver.CoreEngine, StringComparison.InvariantCultureIgnoreCase))
            {
                return GraphProtocol.GraphSON3;
            }

            return null;
        }

        private GraphProtocol? GetByLanguage(IGraphStatement statement, GraphOptions options)
        {
            var language = statement.GraphLanguage ?? options.Language;

            if (language == GraphOptions.GremlinGroovy)
            {
                return GraphProtocol.GraphSON1;
            }

            return null;
        }
    }
}