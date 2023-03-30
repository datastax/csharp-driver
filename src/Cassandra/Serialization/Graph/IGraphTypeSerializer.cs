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
using Cassandra.DataStax.Graph.Internal;
using Cassandra.Serialization.Graph.GraphSON2;
using Newtonsoft.Json.Linq;

namespace Cassandra.Serialization.Graph
{
    /// <summary>
    /// This is the point of entry for the serialization and deserialization logic
    /// (including the type conversion functionality).
    /// </summary>
    internal interface IGraphTypeSerializer
    {
        /// <summary>
        /// <para>
        /// When this is true, the type serializer will deserialize objects as <see cref="GraphNode"/>
        /// when the requested type is object.
        /// </para>
        /// <para>
        /// This is used by the fluent driver (set to false) to force the deserialization of all
        /// inner properties to the actual types instead of returning GraphNode objects. This is necessary
        /// because the GLV serializers call <see cref="IGraphSONReader.ToObject"/> which is implemented by
        /// <see cref="GraphTypeSerializer"/> with a call to <see cref="FromDb(JToken,Type)"/> (with "object" as the requested type).
        /// No type conversion will be made since the requested type is object.
        /// </para>
        /// </summary>
        bool DefaultDeserializeGraphNodes { get; }

        GraphProtocol GraphProtocol { get; }

        /// <summary>
        /// Returns the row parser according to the <see cref="GraphProtocol"/>. This will be passed
        /// to the graph result set.
        /// </summary>
        Func<Row, GraphNode> GetGraphRowParser();

        /// <summary>
        /// Performs deserialization of the provided token and attempts to convert the
        /// deserialized object to the provided type. 
        /// </summary>
        object FromDb(JToken token, Type type);

        /// <summary>
        /// Overload of <see cref="FromDb(JToken,Type)"/> that allows the caller to override
        /// <see cref="DefaultDeserializeGraphNodes"/>.
        /// </summary>
        object FromDb(JToken token, Type type, bool deserializeGraphNodes);
        
        /// <summary>
        /// Generic version of <see cref="FromDb(JToken,Type)"/>
        /// </summary>
        T FromDb<T>(JToken token);

        /// <summary>
        /// Serializes the provided object to GraphSON.
        /// </summary>
        string ToDb(object obj);

        /// <summary>
        /// Attempts to convert the provided object to the target type.
        /// </summary>
        /// <returns>True if conversion was successful (output in the out parameter).</returns>
        bool ConvertFromDb(object obj, Type targetType, out dynamic result);
    }
}