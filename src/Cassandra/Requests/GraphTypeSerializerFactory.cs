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
using Cassandra.DataStax.Graph.Internal;
using Cassandra.Mapping.TypeConversion;
using Cassandra.Serialization.Graph;
using Cassandra.Serialization.Graph.GraphSON1;
using Cassandra.Serialization.Graph.GraphSON2;
using Cassandra.SessionManagement;

namespace Cassandra.Requests
{
    /// <inheritdoc />
    internal class GraphTypeSerializerFactory : IGraphTypeSerializerFactory
    {
        private const string CoreEngine = "Core";

        private static readonly Logger Logger = new Logger(typeof(GraphTypeSerializerFactory));

        private static readonly TypeConverter DefaultTypeConverter = new DefaultTypeConverter();
        
        private static readonly IReadOnlyDictionary<string, IGraphSONDeserializer> EmptyDeserializersDict = 
            new Dictionary<string, IGraphSONDeserializer>(0);
        
        private static readonly IReadOnlyDictionary<Type, IGraphSONSerializer> EmptySerializersDict = 
            new Dictionary<Type, IGraphSONSerializer>(0);

        /// <inheritdoc />
        public GraphProtocol GetDefaultGraphProtocol(IInternalSession session, IGraphStatement statement, GraphOptions options)
        {
            var protocol = GetByKeyspaceMetadata(session, statement, options);

            if (protocol != null)
            {
                GraphTypeSerializerFactory.Logger.Verbose(
                    "Resolved graph protocol {0} according to the keyspace metadata.", protocol.Value.GetInternalRepresentation());
                return protocol.Value;
            }

            protocol = GetByLanguage(statement, options);

            if (protocol != null)
            {
                GraphTypeSerializerFactory.Logger.Verbose(
                    "Resolved graph protocol {0} according to the graph language option.", protocol.Value.GetInternalRepresentation());
                return protocol.Value;
            }

            protocol = GraphProtocol.GraphSON2;

            GraphTypeSerializerFactory.Logger.Verbose(
                "Unable to resolve graph protocol according to keyspace metadata or graph language. " +
                "Resolved graph protocol to {0}.", protocol.Value.GetInternalRepresentation());
            return protocol.Value;
        }
        
        /// <inheritdoc />
        public IGraphTypeSerializer CreateSerializer(
            IReadOnlyDictionary<GraphProtocol, IReadOnlyDictionary<string, IGraphSONDeserializer>> customDeserializers,
            IReadOnlyDictionary<GraphProtocol, IReadOnlyDictionary<Type, IGraphSONSerializer>> customSerializers,
            GraphProtocol graphProtocol,
            bool deserializeGraphNodes)
        {
            switch (graphProtocol)
            {
                case GraphProtocol.GraphSON1:
                    return new GraphSON1TypeSerializer();
                case GraphProtocol.GraphSON2:
                case GraphProtocol.GraphSON3:
                    IReadOnlyDictionary<Type, IGraphSONSerializer> serializers = null;
                    IReadOnlyDictionary<string, IGraphSONDeserializer> deserializers = null;
                    customDeserializers?.TryGetValue(graphProtocol, out deserializers);
                    customSerializers?.TryGetValue(graphProtocol, out serializers);
                    return new GraphTypeSerializer(
                        GraphTypeSerializerFactory.DefaultTypeConverter,
                        graphProtocol,
                        deserializers,
                        serializers,
                        deserializeGraphNodes);

                default:
                    throw new DriverInternalError($"Invalid graph protocol: {graphProtocol.GetInternalRepresentation()}");
            }
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
                GraphTypeSerializerFactory.CoreEngine, StringComparison.InvariantCultureIgnoreCase))
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