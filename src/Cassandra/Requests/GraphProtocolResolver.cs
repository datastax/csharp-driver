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
using System.Linq;
using Cassandra.DataStax.Graph;
using Cassandra.Mapping.TypeConversion;
using Cassandra.Serialization.Graph;
using Cassandra.Serialization.Graph.GraphSON1;
using Cassandra.Serialization.Graph.GraphSON2;
using Cassandra.SessionManagement;
using Newtonsoft.Json;

namespace Cassandra.Requests
{
    internal class GraphProtocolResolver : IGraphProtocolResolver
    {
        private const string CoreEngine = "Core";

        private static readonly Logger Logger = new Logger(typeof(GraphProtocolResolver));
        
        private readonly Lazy<IGraphSONTypeConverter> _graphSON2Converter;
        private readonly Lazy<IGraphSONTypeConverter> _graphSON3Converter;

        public GraphProtocolResolver()
        {
            _graphSON2Converter = new Lazy<IGraphSONTypeConverter>(
                () => GraphSONTypeConverter.NewGraphSON2Converter(new DefaultTypeConverter()));
            _graphSON3Converter = new Lazy<IGraphSONTypeConverter>(
                () => GraphSONTypeConverter.NewGraphSON3Converter(new DefaultTypeConverter()));
        }

        /// <inheritdoc />
        public GraphProtocol GetDefaultGraphProtocol(IInternalSession session, IGraphStatement statement, GraphOptions options)
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

        /// <inheritdoc />
        public Func<Row, GraphNode> GetGraphRowParser(GraphProtocol graphProtocolVersion)
        {
            Func<Row, GraphNode> factory;
            switch (graphProtocolVersion)
            {
                case GraphProtocol.GraphSON3:
                    factory = 
                        row => 
                            new GraphNode(
                                new GraphSONNode(_graphSON3Converter.Value, row.GetValue<string>("gremlin")));
                    break;
                case GraphProtocol.GraphSON2:
                    factory = 
                        row => 
                            new GraphNode(
                                new GraphSONNode(_graphSON2Converter.Value, row.GetValue<string>("gremlin")));
                    break;
                case GraphProtocol.GraphSON1:
                    factory = row => new GraphNode(new GraphSON1Node(row.GetValue<string>("gremlin"), false));
                    break;
                default:
                    throw new DriverInternalError($"Invalid graph protocol: {graphProtocolVersion.GetInternalRepresentation()}");
            }

            return factory;
        }

        /// <inheritdoc />
        public Func<IDictionary<string, object>, string> GetParametersSerializer(GraphProtocol graphProtocol)
        {
            switch (graphProtocol)
            {
                case GraphProtocol.GraphSON1:
                    return parameters => JsonConvert.SerializeObject(parameters, GraphSON1ContractResolver.Settings);
                case GraphProtocol.GraphSON2:
                    // create a Dictionary instance so that it is serialized as a graphson map
                    return parameters => _graphSON2Converter.Value.ToDb(parameters.ToDictionary(kvp => kvp.Key, kvp => kvp.Value));
                case GraphProtocol.GraphSON3:
                    // create a Dictionary instance so that it is serialized as a graphson map
                    return parameters => _graphSON3Converter.Value.ToDb(parameters.ToDictionary(kvp => kvp.Key, kvp => kvp.Value));
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