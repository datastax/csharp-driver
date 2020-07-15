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
using Newtonsoft.Json.Linq;

namespace Cassandra.Serialization.Graph.GraphSON2.Structure
{
    internal abstract class BaseStructureDeserializer
    {
        protected GraphNode ToGraphNode(Func<JToken, GraphNode> factory, JToken token, string propName, bool required = false)
        {
            var prop = !(token is JObject jobj) ? null : jobj[propName];
            if (prop == null)
            {
                if (!required)
                {
                    return null;
                }
                throw new InvalidOperationException($"Required property {propName} not found: {token}");
            }

            return factory.Invoke(prop);
        }

        protected GraphNode ToGraphNode(Func<JToken, GraphNode> factory, JToken token)
        {
            return factory.Invoke(token);
        }

        protected string ToString(JToken token, string propName, bool required = false)
        {
            var prop = !(token is JObject jobj) ? null : jobj[propName];
            if (prop == null)
            {
                if (!required)
                {
                    return null;
                }
                throw new InvalidOperationException($"Required property {propName} not found: {token}");
            }
            return prop.ToString();
        }
    }
}