//
//  Copyright (C) 2017 DataStax, Inc.
//
//  Please see the license for details:
//  http://www.datastax.com/terms/datastax-dse-driver-license-terms
//

using System;
using System.Collections.Generic;
using System.Linq;
using System.Net;
using System.Numerics;
using System.Reflection;
using System.Runtime.Serialization;
using System.Text;
using System.Threading.Tasks;
using Dse.Geometry;
using Dse.Graph;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;

namespace Dse.Serialization.Graph
{
    internal class GraphJsonConverter : JsonConverter
    {
        private delegate object ReadDelegate(JsonReader reader, JsonSerializer serializer);
        private delegate void WriteDelegate(JsonWriter writer, object value, JsonSerializer serializer);

        private static readonly Dictionary<Type, ReadDelegate> Readers = new Dictionary<Type, ReadDelegate>
        {
            { typeof(IPAddress), (r, _) => IPAddress.Parse(r.Value.ToString()) },
            { typeof(BigInteger), (r, _) => BigInteger.Parse(r.Value.ToString()) },
            { typeof(Point), (r, _) => Point.Parse(r.Value.ToString()) },
            { typeof(LineString), (r, _) => LineString.Parse(r.Value.ToString()) },
            { typeof(Polygon), (r, _) => Polygon.Parse(r.Value.ToString()) }
        };

        private static readonly Dictionary<Type, WriteDelegate> Writers = new Dictionary<Type, WriteDelegate>
        {
            { typeof(GraphNode), WriteGraphNode },
            { typeof(IPAddress), WriteStringValue },
            { typeof(Point), WriteStringValue },
            { typeof(LineString), WriteStringValue },
            { typeof(Polygon), WriteStringValue },
            { typeof(BigInteger), WriteStringRawValue }
        };

        internal static readonly GraphJsonConverter Instance = new GraphJsonConverter();

        private GraphJsonConverter()
        {
            
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
            if (!Readers.TryGetValue(objectType, out readHandler))
            {
                return null;
            }
            return readHandler(reader, serializer);
        }

        public override bool CanConvert(Type objectType)
        {
            return Writers.ContainsKey(objectType);
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

        private static object ReadIpAddress(JsonReader reader, JsonSerializer serializer)
        {
            return IPAddress.Parse(reader.Value.ToString());
        }
    }
}
