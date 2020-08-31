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

using System.Collections.Generic;
using Cassandra.DataStax.Graph.Internal;
using Cassandra.Serialization.Graph.Tinkerpop.Structure.IO.GraphSON;
using Newtonsoft.Json.Linq;

namespace Cassandra.Serialization.Graph.GraphSON3.Dse
{
    internal class Duration3Serializer : IGraphSONSerializer, IGraphSONDeserializer
    {
        private const string Prefix = "dse";
        private const string TypeKey = "Duration";

        public static string TypeName => GraphSONUtil.FormatTypeName(Duration3Serializer.Prefix, Duration3Serializer.TypeKey);

        public Dictionary<string, dynamic> Dictify(dynamic objectData, IGraphSONWriter writer)
        {
            Duration d = objectData;
            var value = new Dictionary<string, dynamic>
            {
                { "months", writer.ToDict(d.Months) },
                { "days", writer.ToDict(d.Days) },
                { "nanos", writer.ToDict(d.Nanoseconds) }
            };
            return GraphSONUtil.ToTypedValue(
                Duration3Serializer.TypeKey, 
                value, 
                Duration3Serializer.Prefix);
        }

        public dynamic Objectify(JToken graphsonObject, IGraphSONReader reader)
        {
            var months = (int) reader.ToObject(graphsonObject["months"]);
            var days = (int) reader.ToObject(graphsonObject["days"]);
            var nanos = (long) reader.ToObject(graphsonObject["nanos"]);
            return new Duration(months, days, nanos);
        }
    }
}