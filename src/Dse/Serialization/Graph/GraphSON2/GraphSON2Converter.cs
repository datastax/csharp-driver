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

namespace Dse.Serialization.Graph.GraphSON2
{
    internal class GraphSON2Converter : GraphSONConverter
    {
        internal const string TypeKey = "@type";
        internal const string ValueKey = "@value";

        internal static readonly GraphSON2Converter Instance = new GraphSON2Converter();
        
        private static readonly DateTimeOffset UnixEpoch = new DateTimeOffset(1970, 1, 1, 0, 0, 0, TimeSpan.Zero);

        private readonly IDictionary<Type, ReadDelegate> _readers;

        private GraphSON2Converter()
        {
            _readers = new Dictionary<Type, ReadDelegate>
            {
                { typeof(short), GetScalarReader<short>() },
                { typeof(short?), GetScalarReader<short?>(true) },
                { typeof(int), GetScalarReader<int>() },
                { typeof(int?), GetScalarReader<int?>(true) },
                { typeof(long), GetScalarReader<long>() },
                { typeof(long?), GetScalarReader<long?>(true) },
                { typeof(float), GetScalarReader<float>() },
                { typeof(float?), GetScalarReader<float?>(true) },
                { typeof(double), GetScalarReader<double>() },
                { typeof(double?), GetScalarReader<double?>(true) },
                { typeof(decimal), GetScalarReader<decimal>() },
                { typeof(decimal?), GetScalarReader<decimal?>(true) },
                { typeof(Guid), GetParserReader(Guid.Parse) },
                { typeof(Guid?), GetParserReader(Guid.Parse, true) },
                { typeof(TimeUuid), GetParserReader(ParseTimeUuid) },
                { typeof(TimeUuid?), GetParserReader(ParseTimeUuid, true) },
                { typeof(IPAddress), GetParserReader(IPAddress.Parse) },
                { typeof(BigInteger), GetParserReader(BigInteger.Parse) },
                { typeof(BigInteger?), GetParserReader(BigInteger.Parse, true) },
                { typeof(Point), GetParserReader(Point.Parse, true) },
                { typeof(LineString), GetParserReader(LineString.Parse, true) },
                { typeof(Polygon), GetParserReader(Polygon.Parse, true) },
                { typeof(Duration), GetParserReader(Duration.Parse) },
                { typeof(Duration?), GetParserReader(Duration.Parse, true) },
                { typeof(LocalDate), GetParserReader(LocalDate.Parse, true) },
                { typeof(LocalTime), GetParserReader(LocalTime.Parse, true) },
                { typeof(byte[]), GetParserReader(Convert.FromBase64String) },
                { typeof(DateTimeOffset), GetValueReader(ToDateTimeOffset) },
                { typeof(DateTimeOffset?), GetValueReader(ToDateTimeOffset) },
                { typeof(GraphNode), GetValueReader(ToGraphNode)},
                { typeof(IGraphNode), GetValueReader(ToGraphNode)},
                { typeof(Vertex), GetValueReader(ToVertex)},
                { typeof(IVertex), GetValueReader(ToVertex)},
                { typeof(Edge), GetValueReader(ToEdge)},
                { typeof(IEdge), GetValueReader(ToEdge)},
                { typeof(Path), GetValueReader(ToPath)},
                { typeof(IPath), GetValueReader(ToPath)},
                { typeof(IVertexProperty), GetValueReader(ToVertexProperty)},
                { typeof(IProperty), GetValueReader(ToProperty)}
            };
        }

        private ReadDelegate GetValueReader<T>(Func<JToken, T> toConverter)
        {
            object ValueReader(JTokenReader reader, JsonSerializer serializer)
            {
                var token = GetValueToken<T>(reader.CurrentToken, true);
                if (IsNullOrUndefined(token))
                {
                    return null;
                }
                return toConverter(token);
            }
            return ValueReader;
        }

        private static ReadDelegate GetScalarReader<T>(bool allowNulls = false)
        {
            object ScalarReader(JTokenReader reader, JsonSerializer serializer)
            {
                var token = GetValueToken<T>(reader.CurrentToken, allowNulls);
                return token.ToObject<T>();
            }

            return ScalarReader;
        }

        private static bool IsNullOrUndefined(JToken token)
        {
            return token.Type == JTokenType.Null || token.Type == JTokenType.Undefined;
        }

        private static JToken GetValueToken<T>(JToken currentToken, bool allowNulls)
        {
            if (IsNullOrUndefined(currentToken))
            {
                if (!allowNulls)
                {
                    throw new InvalidOperationException(
                        $"Cannot create a instance of {typeof(T).Name} from a null value, " +
                        $"use Nullable<{typeof(T).Name}> instead");   
                }
                // return the null token
                return currentToken;
            }
            if (currentToken is JValue)
            {
                return currentToken;
            }
            var token = currentToken[ValueKey];
            if (token == null)
            {
                throw new InvalidOperationException(
                    $"Cannot create a {typeof(T)} instance from {currentToken}");
            }
            return token;
        }

        private static DateTimeOffset ToDateTimeOffset(JToken token)
        {
            var value = token as JValue;
            if (value == null)
            {
                throw new InvalidOperationException($"Cannot create a DateTimeOffset from {token}");
            }
            if (value.Type == JTokenType.Integer)
            {
                return UnixEpoch.AddMilliseconds(value.ToObject<int>());
            }
            return value.ToObject<DateTimeOffset>();
        }

        protected override GraphNode ToGraphNode(JToken token)
        {
            return token == null ? null : new GraphNode(new GraphSON2Node(token));
        }

        private static ReadDelegate GetParserReader<T>(Func<string, T> parser, bool allowNulls = false)
        {
            object ParserReader(JTokenReader reader, JsonSerializer serializer)
            {
                var token = GetValueToken<T>(reader.CurrentToken, allowNulls);
                if (IsNullOrUndefined(token))
                {
                    if (!allowNulls)
                    {
                        throw new InvalidOperationException(
                            $"Cannot create a instance of {typeof(T).Name} from a null value, " +
                            $"use Nullable<{typeof(T).Name}> instead");   
                    }
                    return null;
                }
                return parser(token.ToString());
            }
            return ParserReader;
        }

        public override void WriteJson(JsonWriter writer, object value, JsonSerializer serializer)
        {
            throw new NotSupportedException($"{nameof(GraphSON2Converter)} should not be used for serialization");
        }

        public override object ReadJson(JsonReader reader, Type objectType, object existingValue, 
                                        JsonSerializer serializer)
        {
            ReadDelegate readHandler;
            if (!_readers.TryGetValue(objectType, out readHandler))
            {
                throw new NotSupportedException($"The Type '{objectType.Name}' is not supported");
            }
            return readHandler((JTokenReader) reader, serializer);
        }

        public override bool CanConvert(Type objectType)
        {
            return _readers.ContainsKey(objectType);
        }
    }
}
