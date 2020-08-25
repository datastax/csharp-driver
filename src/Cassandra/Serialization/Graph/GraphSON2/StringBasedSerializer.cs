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

namespace Cassandra.Serialization.Graph.GraphSON2
{
    internal abstract class StringBasedSerializer : IGraphSONSerializer, IGraphSONDeserializer
    {
        private readonly string _typeKey;
        private readonly string _prefix;

        protected StringBasedSerializer(string prefix, string typeKey)
        {
            _typeKey = typeKey;
            _prefix = prefix;
        }
        
        public Dictionary<string, dynamic> Dictify(dynamic objectData, IGraphSONWriter writer)
        {
            return GraphSONUtil.ToTypedValue(_typeKey, objectData == null ? null : ToString(objectData), _prefix);
        }

        public dynamic Objectify(JToken graphsonObject, IGraphSONReader reader)
        {
            var str = TokenToString(graphsonObject);
            if (str == null)
            {
                return null;
            }

            return FromString(str);
        }

        protected virtual string TokenToString(JToken token)
        {
            return token.ToObject<string>();
        }
        
        protected abstract string ToString(dynamic obj);

        protected abstract dynamic FromString(string str);
    }
}