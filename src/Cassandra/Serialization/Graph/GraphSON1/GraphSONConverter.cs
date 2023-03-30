//
//      Copyright (C) DataStax Inc.
//
//   Licensed under the Apache License, Version 2.0 (the "License");
//   you may not use this file except in compliance with the License.
//   You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
//   Unless required by applicable law or agreed to in writing, software
//   distributed under the License is distributed on an "AS IS" BASIS,
//   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//   See the License for the specific language governing permissions and
//   limitations under the License.
//

using System;
using System.Collections.Generic;
using System.Collections.ObjectModel;
using System.Linq;
using Cassandra.DataStax.Graph;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;

namespace Cassandra.Serialization.Graph.GraphSON1
{
    internal abstract class GraphSONConverter : JsonConverter
    {
        private static readonly IDictionary<string, GraphNode> EmptyProperties =
            new ReadOnlyDictionary<string, GraphNode>(new Dictionary<string, GraphNode>());

        protected delegate object ReadDelegate(JTokenReader reader, JsonSerializer serializer);

        protected delegate void WriteDelegate(JsonWriter writer, object value, JsonSerializer serializer);

        protected abstract GraphNode ToGraphNode(JToken token);

        private GraphNode ToGraphNode(JToken token, string propName, bool required = false)
        {
            var prop = token[propName];
            if (prop == null)
            {
                if (!required)
                {
                    return null;
                }
                throw new InvalidOperationException($"Required property {propName} not found: {token}");
            }
            return ToGraphNode(prop);
        }

        private string ToString(JToken token, string propName, bool required = false)
        {
            var prop = token[propName];
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

        protected Vertex ToVertex(JToken token)
        {
            var properties = GraphSONConverter.EmptyProperties;
            var propertiesJsonProp = token["properties"] as JObject;
            if (propertiesJsonProp != null)
            {
                properties = propertiesJsonProp
                    .Properties()
                    .ToDictionary(prop => prop.Name, prop => ToGraphNode(prop.Value));
            }
            return new Vertex(
                ToGraphNode(token, "id", true),
                ToString(token, "label", true),
                properties);
        }

        protected Edge ToEdge(JToken token)
        {
            var properties = GraphSONConverter.EmptyProperties;
            var propertiesJsonProp = token["properties"] as JObject;
            if (propertiesJsonProp != null)
            {
                properties = propertiesJsonProp
                    .Properties()
                    .ToDictionary(prop => prop.Name, prop => ToGraphNode(prop.Value));
            }
            return new Edge(
                ToGraphNode(token, "id", true),
                ToString(token, "label", true),
                properties,
                ToGraphNode(token, "inV"),
                ToString(token, "inVLabel"),
                ToGraphNode(token, "outV"),
                ToString(token, "outVLabel"));
        }

        protected Path ToPath(JToken token)
        {
            ICollection<ICollection<string>> labels = null;
            ICollection<GraphNode> objects = null;
            var labelsProp = token["labels"] as JArray;
            if (labelsProp != null)
            {
                // labels prop is a js Array<Array<string>>
                labels = labelsProp
                    .Select(node =>
                    {
                        var arrayNode = node as JArray;
                        if (arrayNode == null)
                        {
                            throw new InvalidOperationException($"Cannot create an Path from {token}");
                        }
                        return arrayNode.Select(n => n.ToString()).ToArray();
                    })
                    .ToArray();
            }
            var objectsProp = token["objects"] as JArray;
            if (objectsProp != null)
            {
                // labels prop is a js Array<object>
                objects = objectsProp.Select(ToGraphNode).ToArray();
            }
            return new Path(labels, objects);
        }

        protected IVertexProperty ToVertexProperty(JToken token)
        {
            var graphNode = ToGraphNode(token);
            return new VertexProperty(
                graphNode.Get<GraphNode>("id", true), graphNode.Get<string>("label"),
                graphNode.Get<GraphNode>("value", true),
                graphNode.Get<GraphNode>("vertex"),
                graphNode.Get<GraphNode>("properties")?.GetProperties() ?? GraphSONConverter.EmptyProperties);
        }

        protected IProperty ToProperty(JToken token)
        {
            return new Property(
                ToString(token, "key", true),
                ToGraphNode(token, "value"),
                ToGraphNode(token, "element"));
        }

        protected TimeUuid ParseTimeUuid(string value)
        {
            return Guid.Parse(value);
        }
    }
}
