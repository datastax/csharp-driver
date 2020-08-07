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
using Newtonsoft.Json.Linq;

namespace Cassandra.Serialization.Graph.GraphSON3.Dse
{
    internal interface IComplexTypeGraphSONDeserializer
    {
        /// <summary>
        ///     Deserializes GraphSON UDT to an object.
        /// </summary>
        /// <param name="graphsonObject">The GraphSON udt object to objectify.</param>
        /// <param name="type">Target type.</param>
        /// <param name="serializer">The graph type serializer instance.</param>
        /// <param name="genericSerializer">Generic serializer instance from which UDT Mappings can be obtained.</param>
        /// <returns>The deserialized object.</returns>
        dynamic Objectify(JToken graphsonObject, Type type, IGraphTypeSerializer serializer, IGenericSerializer genericSerializer);
    }
}