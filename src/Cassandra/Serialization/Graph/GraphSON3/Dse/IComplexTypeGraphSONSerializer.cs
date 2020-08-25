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

namespace Cassandra.Serialization.Graph.GraphSON3.Dse
{
    internal interface IComplexTypeGraphSONSerializer
    {
        /// <summary>
        ///     Transforms an object into a dictionary that resembles its GraphSON representation.
        /// </summary>
        /// <param name="objectData">The object to dictify.</param>
        /// <param name="serializer">The graph type serializer instance.</param>
        /// <param name="genericSerializer">Generic serializer instance from which UDT Mappings can be obtained.</param>
        /// <param name="result">The GraphSON representation.</param>
        /// <returns>True if this object is a UDT and serialization was successful. False if this object is not a UDT.</returns>
        bool TryDictify(
            dynamic objectData,
            IGraphSONWriter serializer,
            IGenericSerializer genericSerializer,
            out dynamic result);
    }
}