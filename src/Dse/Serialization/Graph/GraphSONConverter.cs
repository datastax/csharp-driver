//
//  Copyright (C) 2017 DataStax, Inc.
//
//  Please see the license for details:
//  http://www.datastax.com/terms/datastax-dse-driver-license-terms
//

using System;
using System.Collections.Generic;
using System.Linq;
using Dse.Graph;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;

namespace Dse.Serialization.Graph
{
    internal abstract class GraphSONConverter : JsonConverter
    {
        protected delegate object ReadDelegate(JTokenReader reader, JsonSerializer serializer);

        protected delegate void WriteDelegate(JsonWriter writer, object value, JsonSerializer serializer);

        protected abstract GraphNode ToGraphNode(JToken token);

        protected GraphNode ToGraphNode(JToken token, string propName, bool required = false)
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

        protected string ToString(JToken token, string propName, bool required = false)
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
            IDictionary<string, GraphNode> properties = null;
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
            IDictionary<string, GraphNode> properties = null;
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

        protected TimeUuid ParseTimeUuid(string value)
        {
            return Guid.Parse(value);
        }
    }
}
