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
using System.Collections.Generic;

using Cassandra.DataStax.Graph;

using Newtonsoft.Json.Linq;

namespace Cassandra.Serialization.Graph.Dse
{
    internal abstract class BaseDeserializer
    {
        private readonly Func<JToken, GraphNode> _graphNodeFactory;
        
        protected BaseDeserializer(Func<JToken, GraphNode> graphNodeFactory)
        {
            _graphNodeFactory = graphNodeFactory;
        }

        protected GraphNode ToGraphNode(JToken token, string propName, bool required = false)
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

            return _graphNodeFactory.Invoke(prop);
        }

        protected GraphNode ToGraphNode(JToken token)
        {
            return _graphNodeFactory.Invoke(token);
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