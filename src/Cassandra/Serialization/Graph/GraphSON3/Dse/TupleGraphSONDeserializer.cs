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
    /// <inheritdoc />
    internal class TupleGraphSONDeserializer : IComplexTypeGraphSONDeserializer
    {
        /// <inheritdoc />
        public dynamic Objectify(
            JToken graphsonObject, Type type, IGraphTypeSerializer serializer, IGenericSerializer genericSerializer)
        {
            if (!Utils.IsTuple(type))
            {
                throw new InvalidOperationException($"Can not deserialize a tuple to {type.FullName}.");
            }

            var values = (JArray)graphsonObject["value"];

            var genericArguments = type.GetGenericArguments();

            if (genericArguments.Length != values.Count)
            {
                throw new InvalidOperationException(
                    "Could not deserialize tuple, number of elements don't match " +
                    $"(expected {genericArguments.Length} but the server returned {values.Count}).");
            }
            var tupleValues = new object[values.Count];
            for (var i = 0; i < tupleValues.Length; i++)
            {
                tupleValues[i] = serializer.FromDb(values[i], genericArguments[i], false);
            }

            return Activator.CreateInstance(type, tupleValues);
        }
    }
}