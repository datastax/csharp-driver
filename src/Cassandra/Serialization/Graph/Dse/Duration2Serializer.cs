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

using Cassandra.Serialization.Graph.Tinkerpop.Structure.IO.GraphSON;

namespace Cassandra.Serialization.Graph.Dse
{
    internal class Duration2Serializer : StringBasedSerializer
    {
        private const string Prefix = "gx";
        private const string TypeKey = "Duration";

        public Duration2Serializer() : base(Duration2Serializer.Prefix, Duration2Serializer.TypeKey)
        {
        }

        public static string TypeName => GraphSONUtil.FormatTypeName(Duration2Serializer.Prefix, Duration2Serializer.TypeKey);

        protected override string ToString(dynamic obj)
        {
            Duration tinkerpopInstant = obj;
            return tinkerpopInstant.ToJavaDurationString();
        }

        protected override dynamic FromString(string str)
        {
            return Duration.Parse(str);
        }
    }
}