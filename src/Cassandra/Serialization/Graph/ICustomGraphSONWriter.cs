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

using Cassandra.DataStax.Graph.Internal;
using Cassandra.Serialization.Graph.GraphSON2;
using Cassandra.Serialization.Graph.Tinkerpop.Structure.IO.GraphSON;

namespace Cassandra.Serialization.Graph
{
    /// <summary>
    /// <para>
    /// This interface is similar to <see cref="IGraphSONWriter"/> but the serializers do NOT
    /// depend on this one while they depend on <see cref="IGraphSONWriter"/>.
    /// </para>
    /// <para>
    /// This interface is implemented by the custom GraphSON writer (<see cref="CustomGraphSON2Writer"/>)
    /// which is an imported version of the Tinkerpop's <see cref="GraphSONWriter"/> with a few changes.
    /// </para>
    /// <para>
    /// See XML docs of <see cref="GraphTypeSerializer"/> for more information.
    /// </para>
    /// </summary>
    internal interface ICustomGraphSONWriter
    {
        bool TryToDict(dynamic objectData, out dynamic result);
    }
}