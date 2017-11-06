//
//  Copyright (C) 2017 DataStax, Inc.
//
//  Please see the license for details:
//  http://www.datastax.com/terms/datastax-dse-driver-license-terms
//

using System;
using System.Collections.Generic;
using System.Net;
using System.Numerics;
using Dse.Geometry;
using Dse.Graph;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;

namespace Dse.Serialization.Graph.GraphSON1
{
    internal class GraphSON1Converter : GraphSONConverter
    {
        private static readonly Dictionary<Type, WriteDelegate> Writers = new Dictionary<Type, WriteDelegate>
        {
            { typeof(GraphNode), WriteGraphNode },
            { typeof(IPAddress), WriteStringValue },
            { typeof(Point), WriteStringValue },
            { typeof(LineString), WriteStringValue },
            { typeof(Polygon), WriteStringValue },
            { typeof(BigInteger), WriteStringRawValue },
            { typeof(Duration), WriteDuration },
            { typeof(LocalDate), WriteStringValue },
            { typeof(LocalTime), WriteStringValue }
        };

        internal static readonly GraphSON1Converter Instance = new GraphSON1Converter();
        
        private readonly Dictionary<Type, ReadDelegate> _readers;

        private GraphSON1Converter()
        {
            _readers = new Dictionary<Type, ReadDelegate>
            {
                { typeof(IPAddress), (r, _) => IPAddress.Parse(r.Value.ToString()) },
                { typeof(BigInteger), (r, _) => BigInteger.Parse(r.Value.ToString()) },
                { typeof(Point), (r, _) => Point.Parse(r.Value.ToString()) },
                { typeof(LineString), (r, _) => LineString.Parse(r.Value.ToString()) },
                { typeof(Polygon), (r, _) => Polygon.Parse(r.Value.ToString()) },
                { typeof(Duration), (r, _) => Duration.Parse(r.Value.ToString()) },
                { typeof(LocalDate), (r, _) => LocalDate.Parse(r.Value.ToString()) },
                { typeof(LocalTime), (r, _) => LocalTime.Parse(r.Value.ToString()) },
                { typeof(GraphNode), GetTokenReader(t => new GraphNode(new GraphSON1Node(t))) },
                { typeof(IGraphNode), GetTokenReader(t => new GraphNode(new GraphSON1Node(t))) },
                { typeof(Vertex), GetTokenReader(ToVertex) },
                { typeof(IVertex), GetTokenReader(ToVertex) },
                { typeof(Edge), GetTokenReader(ToEdge) },
                { typeof(IEdge), GetTokenReader(ToEdge) },
                { typeof(Path), GetTokenReader(ToPath) },
                { typeof(IPath), GetTokenReader(ToPath) },
                { typeof(IVertexProperty), GetTokenReader(ToVertexProperty) },
                { typeof(IProperty), GetTokenReader(ToProperty) }
            };
        }

        private ReadDelegate GetTokenReader<T>(Func<JToken, T> tokenReader)
        {
            object TokenReader(JTokenReader reader, JsonSerializer serializer)
            {
                var token = reader.CurrentToken;
                if (!(token is JObject))
                {
                    throw new InvalidOperationException($"Cannot create a {typeof(T).Name} from '{token}'");
                }
                return tokenReader(token);
            }
            return TokenReader;
        }

        public override void WriteJson(JsonWriter writer, object value, JsonSerializer serializer)
        {
            WriteDelegate writeHandler;
            if (!Writers.TryGetValue(value.GetType(), out writeHandler))
            {
                return;
            }
            writeHandler(writer, value, serializer);
        }

        public override object ReadJson(JsonReader reader, Type objectType, object existingValue, JsonSerializer serializer)
        {
            ReadDelegate readHandler;
            if (!_readers.TryGetValue(objectType, out readHandler))
            {
                return null;
            }
            return readHandler((JTokenReader)reader, serializer);
        }

        protected override GraphNode ToGraphNode(JToken token)
        {
            return token == null ? null : new GraphNode(new GraphSON1Node(token));
        }

        public override bool CanConvert(Type objectType)
        {
            return _readers.ContainsKey(objectType);
        }

        private static void WriteStringValue(JsonWriter writer, object value, JsonSerializer serializer)
        {
            // GraphSON1 uses string representation for geometry types
            writer.WriteValue(value.ToString());
        }

        private static void WriteStringRawValue(JsonWriter writer, object value, JsonSerializer serializer)
        {
            // ie: BigInteger
            writer.WriteRawValue(value.ToString());
        }

        private static void WriteGraphNode(JsonWriter writer, object value, JsonSerializer serializer)
        {
            ((GraphNode)value).WriteJson(writer, serializer);
        }

        private static void WriteDuration(JsonWriter writer, object value, JsonSerializer serializer)
        {
            var durationString = ((Duration)value).ToJavaDurationString();
            writer.WriteValue(durationString);
        }
    }
}
