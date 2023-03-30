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
using Cassandra.Serialization.Graph;
using Cassandra.SessionManagement;

namespace Cassandra.Requests
{
    /// <summary>
    /// Builds graph type serializers according to the protocol version. Also exposes a method
    /// to compute the default graph protocol according to the keyspace metadata.
    /// </summary>
    internal interface IGraphTypeSerializerFactory
    {
        /// <summary>
        /// Resolves the graph protocol version according to the keyspace metadata or language.
        /// See <see cref="GraphOptions.GraphProtocolVersion"/> for an explanation.
        /// </summary>
        GraphProtocol GetDefaultGraphProtocol(IInternalSession session, IGraphStatement statement, GraphOptions options);
        
        /// <summary>
        /// Gets the serializer according to the protocol version.
        /// </summary>
        IGraphTypeSerializer CreateSerializer(
            IInternalSession session,
            IReadOnlyDictionary<GraphProtocol, IReadOnlyDictionary<string, IGraphSONDeserializer>> customDeserializers,
            IReadOnlyDictionary<GraphProtocol, IReadOnlyDictionary<Type, IGraphSONSerializer>> customSerializers,
            GraphProtocol graphProtocolVersion,
            bool deserializeGraphNodes);
    }
}