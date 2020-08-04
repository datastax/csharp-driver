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
using Cassandra.Serialization.Graph.Tinkerpop.Structure.IO.GraphSON;
using Newtonsoft.Json.Linq;

namespace Cassandra.Serialization.Graph
{
    /// <summary>
    ///     Supports deserializing GraphSON into an object that requires a graph node factory.
    /// This is an adaptation of the Tinkerpop's <see cref="IGraphSONDeserializer"/> interface. The
    /// <see cref="CustomGraphSON2Reader"/> adds support for these custom deserializers on top of the imported
    /// functionality from <see cref="GraphSONReader"/> that handles the standard deserializers.
    /// </summary>
    internal interface IGraphSONStructureDeserializer
    {
        /// <summary>
        ///     Deserializes GraphSON to an object.
        /// </summary>
        /// <param name="graphsonObject">The GraphSON object to objectify.</param>
        /// <param name="graphNodeFactory">Graph Node factory that can be used to build graph node objects.</param>
        /// <param name="reader">A <see cref="GraphSONReader" /> that can be used to objectify properties of the GraphSON object.</param>
        /// <returns>The deserialized object.</returns>
        dynamic Objectify(JToken graphsonObject, Func<JToken, GraphNode> graphNodeFactory, IGraphSONReader reader);
    }
}