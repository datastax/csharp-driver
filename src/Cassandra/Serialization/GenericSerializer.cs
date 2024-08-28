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
using System.Collections;
using System.Collections.Generic;
using System.Linq;
using System.Numerics;
using System.Reflection;
using Cassandra.Serialization.Geometry;
using Cassandra.Serialization.Search;

namespace Cassandra.Serialization
{
    internal class GenericSerializer : IGenericSerializer
    {
        private static readonly Logger Logger = new Logger(typeof (GenericSerializer));

        private readonly IDictionary<ColumnTypeCode, ITypeSerializer> _primitiveDeserializers = new Dictionary<ColumnTypeCode, ITypeSerializer>
        {
            { ColumnTypeCode.Ascii, TypeSerializer.PrimitiveAsciiStringSerializer },
            { ColumnTypeCode.Bigint, TypeSerializer.PrimitiveLongSerializer },
            { ColumnTypeCode.Blob, TypeSerializer.PrimitiveByteArraySerializer },
            { ColumnTypeCode.Boolean, TypeSerializer.PrimitiveBooleanSerializer },
            { ColumnTypeCode.Counter, TypeSerializer.PrimitiveLongSerializer },
            { ColumnTypeCode.Date, TypeSerializer.PrimitiveLocalDateSerializer },
            { ColumnTypeCode.Decimal, TypeSerializer.PrimitiveDecimalSerializer },
            { ColumnTypeCode.Double, TypeSerializer.PrimitiveDoubleSerializer},
            { ColumnTypeCode.Duration, TypeSerializer.PrimitiveDurationSerializer },
            { ColumnTypeCode.Float, TypeSerializer.PrimitiveFloatSerializer },
            { ColumnTypeCode.Inet, TypeSerializer.PrimitiveIpAddressSerializer },
            { ColumnTypeCode.Int, TypeSerializer.PrimitiveIntSerializer },
            { ColumnTypeCode.SmallInt, TypeSerializer.PrimitiveShortSerializer },
            { ColumnTypeCode.Text, TypeSerializer.PrimitiveStringSerializer },
            { ColumnTypeCode.Time, TypeSerializer.PrimitiveLocalTimeSerializer},
            { ColumnTypeCode.Timestamp, TypeSerializer.PrimitiveDateTimeOffsetSerializer },
            { ColumnTypeCode.Timeuuid, TypeSerializer.PrimitiveGuidSerializer },
            { ColumnTypeCode.TinyInt, TypeSerializer.PrimitiveSbyteSerializer },
            { ColumnTypeCode.Uuid, TypeSerializer.PrimitiveGuidSerializer },
            { ColumnTypeCode.Varchar, TypeSerializer.PrimitiveStringSerializer },
            { ColumnTypeCode.Varint, TypeSerializer.PrimitiveBigIntegerSerializer }
        };
        
        private readonly IEnumerable<ITypeSerializer> _defaultCustomTypeSerializers = new ITypeSerializer[]
        {
            new DateRangeSerializer(),
            new DurationSerializer(true),
            new LineStringSerializer(),
            new PointSerializer(),
            new PolygonSerializer(),
        };

        private readonly Dictionary<Type, ITypeSerializer> _primitiveSerializers = new Dictionary<Type, ITypeSerializer>();
        private readonly IDictionary<IColumnInfo, ITypeSerializer> _customDeserializers = new Dictionary<IColumnInfo, ITypeSerializer>();
        private readonly IDictionary<Type, ITypeSerializer> _customSerializers = new Dictionary<Type, ITypeSerializer>();
        private readonly CollectionSerializer _collectionSerializer = new CollectionSerializer();
        private readonly DictionarySerializer _dictionarySerializer = new DictionarySerializer();
        private readonly VectorSerializer _vectorSerializer = new VectorSerializer();
        private readonly TupleSerializer _tupleSerializer = new TupleSerializer();
        private readonly Dictionary<ColumnTypeCode, Func<IColumnInfo, Type>> _defaultTypes = new Dictionary<ColumnTypeCode, Func<IColumnInfo, Type>>();
        private readonly Dictionary<ColumnTypeCode, Func<IColumnInfo, Type>> _defaultGraphTypes;
        //Udt serializer can be specified
        private UdtSerializer _udtSerializer = new UdtSerializer();

        internal GenericSerializer(IEnumerable<ITypeSerializer> typeSerializers = null)
        {
            InitPrimitiveSerializers();
            _collectionSerializer.SetChildSerializer(this);
            _dictionarySerializer.SetChildSerializer(this);
            _vectorSerializer.SetChildSerializer(this);
            _tupleSerializer.SetChildSerializer(this);
            _udtSerializer.SetChildSerializer(this);
            InitDefaultTypes();
            InitTypeAdapters();
            _defaultGraphTypes = new Dictionary<ColumnTypeCode, Func<IColumnInfo, Type>>(_defaultTypes)
            {
                [ColumnTypeCode.Set] = _collectionSerializer.GetClrTypeForGraphSet,
                [ColumnTypeCode.List] = _collectionSerializer.GetClrTypeForGraphList,
                [ColumnTypeCode.Map] = _dictionarySerializer.GetClrTypeForGraph,
                [ColumnTypeCode.Tuple] = _tupleSerializer.GetClrTypeForGraph
            };
            SetSpecificSerializers(typeSerializers);
        }

        public object Deserialize(ProtocolVersion version, byte[] buffer, int offset, int length, ColumnTypeCode typeCode, IColumnInfo typeInfo)
        {
            if (_primitiveDeserializers.TryGetValue(typeCode, out ITypeSerializer typeSerializer))
            {
                return typeSerializer.Deserialize((byte)version, buffer, offset, length, typeInfo);
            }
            switch (typeCode)
            {
                case ColumnTypeCode.Custom:
                {
                    if (typeInfo is VectorColumnInfo)
                    {
                        return _vectorSerializer.Deserialize((byte)version, buffer, offset, length, typeInfo);
                    }
                    if (_customDeserializers.Count == 0 || !_customDeserializers.TryGetValue(typeInfo, out typeSerializer))
                    {
                        // Use byte[] by default
                        typeSerializer = TypeSerializer.PrimitiveByteArraySerializer;
                    }
                    return typeSerializer.Deserialize((byte)version, buffer, offset, length, typeInfo);
                }
                case ColumnTypeCode.Udt:
                    return _udtSerializer.Deserialize((byte)version, buffer, offset, length, typeInfo);
                case ColumnTypeCode.List:
                case ColumnTypeCode.Set:
                    return _collectionSerializer.Deserialize((byte)version, buffer, offset, length, typeInfo);
                case ColumnTypeCode.Map:
                    return _dictionarySerializer.Deserialize((byte)version, buffer, offset, length, typeInfo);
                case ColumnTypeCode.Tuple:
                    return _tupleSerializer.Deserialize((byte)version, buffer, offset, length, typeInfo);
            }
            //Unknown type, return the byte representation
            return buffer;
        }

        public Type GetClrType(ColumnTypeCode typeCode, IColumnInfo typeInfo)
        {
            if (!_defaultTypes.TryGetValue(typeCode, out Func<IColumnInfo, Type> clrTypeHandler))
            {
                throw new ArgumentException($"No handler defined for type {typeCode}");
            }
            return clrTypeHandler(typeInfo);
        }
        
        public Type GetClrTypeForGraph(ColumnTypeCode typeCode, IColumnInfo typeInfo)
        {
            if (!_defaultGraphTypes.TryGetValue(typeCode, out Func<IColumnInfo, Type> clrTypeHandler))
            {
                throw new ArgumentException($"No handler defined for type {typeCode}");
            }
            return clrTypeHandler(typeInfo);
        }

        public Type GetClrTypeForCustom(IColumnInfo typeInfo)
        {
            if (typeInfo is VectorColumnInfo vectorColumnInfo)
            {
                return _vectorSerializer.GetClrType(vectorColumnInfo);
            }
            var customTypeInfo = (CustomColumnInfo)typeInfo;
            if (customTypeInfo.CustomTypeName == null || !customTypeInfo.CustomTypeName.StartsWith(DataTypeParser.UdtTypeName))
            {
                if (_customDeserializers.TryGetValue(customTypeInfo, out ITypeSerializer serializer))
                {
                    return serializer.Type;
                }
                return TypeSerializer.PrimitiveByteArraySerializer.Type;
            }
            var dataType = DataTypeParser.ParseFqTypeName(customTypeInfo.CustomTypeName);
            var map = _udtSerializer.GetUdtMap($"{dataType.Keyspace}.{dataType.Name}");
            if (map == null)
            {
                throw new InvalidTypeException($"No mapping defined for udt type {dataType.Keyspace}.{dataType.Name}");
            }
            return map.NetType;
        }

        public ColumnTypeCode GetCqlType(Type type, out IColumnInfo typeInfo)
        {
            typeInfo = null;
            if (_primitiveSerializers.TryGetValue(type, out ITypeSerializer typeSerializer))
            {
                return typeSerializer.CqlType;
            }
            if (_customSerializers.Count > 0 && _customSerializers.TryGetValue(type, out typeSerializer))
            {
                typeInfo = typeSerializer.TypeInfo;
                return typeSerializer.CqlType;
            }
            if (type.IsArray)
            {
                ColumnTypeCode valueTypeCode = GetCqlType(type.GetElementType(), out IColumnInfo valueTypeInfo);
                typeInfo = new ListColumnInfo
                {
                    ValueTypeCode = valueTypeCode,
                    ValueTypeInfo = valueTypeInfo
                };
                return ColumnTypeCode.List;
            }
            if (type.GetTypeInfo().IsGenericType)
            {
                var genericTypeDefinition = type.GetGenericTypeDefinition();
                if (genericTypeDefinition == typeof(Nullable<>))
                {
                    return GetCqlType(type.GetTypeInfo().GetGenericArguments()[0], out typeInfo);
                }

                if (genericTypeDefinition == typeof(CqlVector<>))
                {
                    var valueTypeCode = GetCqlType(type.GetTypeInfo().GetGenericArguments()[0], out var vectorSubtypeInfo);
                    typeInfo = new VectorColumnInfo
                    {
                        ValueTypeCode = valueTypeCode,
                        ValueTypeInfo = vectorSubtypeInfo,
                        Dimension = null // can't know the dimension just from the CLR type
                    };
                    return ColumnTypeCode.Custom;
                }
                if (typeof(IEnumerable).GetTypeInfo().IsAssignableFrom(type))
                {
                    IColumnInfo valueTypeInfo;
                    ColumnTypeCode valueTypeCode;
                    var interfaces = type.GetTypeInfo().GetInterfaces();
                    if (typeof(IDictionary).GetTypeInfo().IsAssignableFrom(type) && 
                        interfaces.Any(t => IntrospectionExtensions.GetTypeInfo(t).IsGenericType && t.GetGenericTypeDefinition() == typeof(IDictionary<,>)))
                    {
                        var keyTypeCode = GetCqlType(type.GetTypeInfo().GetGenericArguments()[0], out IColumnInfo keyTypeInfo);
                        valueTypeCode = GetCqlType(type.GetTypeInfo().GetGenericArguments()[1], out valueTypeInfo);
                        typeInfo = new MapColumnInfo
                        {
                            KeyTypeCode = keyTypeCode,
                            KeyTypeInfo = keyTypeInfo,
                            ValueTypeCode = valueTypeCode,
                            ValueTypeInfo = valueTypeInfo
                        };
                        return ColumnTypeCode.Map;
                    }
                    if (interfaces.Any(t => t.GetTypeInfo().IsGenericType && t.GetGenericTypeDefinition() == typeof(ISet<>)))
                    {
                        var keyTypeCode = GetCqlType(type.GetTypeInfo().GetGenericArguments()[0], out IColumnInfo keyTypeInfo);
                        typeInfo = new SetColumnInfo
                        {
                            KeyTypeCode = keyTypeCode,
                            KeyTypeInfo = keyTypeInfo
                        };
                        return ColumnTypeCode.Set;
                    }
                    valueTypeCode = GetCqlType(type.GetTypeInfo().GetGenericArguments()[0], out valueTypeInfo);
                    typeInfo = new ListColumnInfo
                    {
                        ValueTypeCode = valueTypeCode,
                        ValueTypeInfo = valueTypeInfo
                    };
                    return ColumnTypeCode.List;
                }
                if (typeof(IStructuralComparable).GetTypeInfo().IsAssignableFrom(type) && type.FullName.StartsWith("System.Tuple"))
                {
                    typeInfo = new TupleColumnInfo
                    {
                        Elements = type.GetTypeInfo().GetGenericArguments().Select(t =>
                        {
                            var dataType = new ColumnDesc
                            {
                                TypeCode = GetCqlType(t, out IColumnInfo tupleSubTypeInfo),
                                TypeInfo = tupleSubTypeInfo
                            };
                            return dataType;
                        }).ToList()
                    };
                    return ColumnTypeCode.Tuple;
                }
            }

            //Determine if its a Udt type
            var udtMap = _udtSerializer.GetUdtMap(type);
            if (udtMap != null)
            {
                typeInfo = udtMap.Definition;
                return ColumnTypeCode.Udt;
            }
            throw new InvalidTypeException($"Unknown Cassandra target type for CLR type {type.FullName}");
        }

        private void InitPrimitiveSerializers()
        {
            //Default primitive serializers
            foreach (var serializer in _primitiveDeserializers.Values)
            {
                _primitiveSerializers[serializer.Type] = serializer;
            }
            //Allow TimeUuid as timeuuid
            _primitiveSerializers.Add(typeof(TimeUuid), TypeSerializer.PrimitiveTimeUuidSerializer);
            //Allow DateTime as timestamp
            _primitiveSerializers.Add(typeof(DateTime), TypeSerializer.PrimitiveDateTimeSerializer);
        }

        private void InitDefaultTypes()
        {
            foreach (var kv in _primitiveDeserializers)
            {
                var clrType = kv.Value.Type;
                _defaultTypes.Add(kv.Key, _ => clrType);
            }
            _defaultTypes.Add(ColumnTypeCode.Custom, GetClrTypeForCustom);
            _defaultTypes.Add(ColumnTypeCode.List, _collectionSerializer.GetClrTypeForList);
            _defaultTypes.Add(ColumnTypeCode.Set, _collectionSerializer.GetClrTypeForSet);
            _defaultTypes.Add(ColumnTypeCode.Map, _dictionarySerializer.GetClrType);
            _defaultTypes.Add(ColumnTypeCode.Udt, _udtSerializer.GetClrType);
            _defaultTypes.Add(ColumnTypeCode.Tuple, _tupleSerializer.GetClrType);
        }

        private void InitTypeAdapters()
        {
            //TypeAdapters was the way we exposed type encoding/decoding extensions since v1
            //Its going to be removed in future versions but we have to support them for now.
            if (TypeAdapters.DecimalTypeAdapter.GetDataType() != typeof(decimal))
            {
                InsertLegacySerializer(ColumnTypeCode.Decimal, TypeAdapters.DecimalTypeAdapter, false);
            }
            if (TypeAdapters.VarIntTypeAdapter.GetDataType() != typeof(BigInteger))
            {
                InsertLegacySerializer(ColumnTypeCode.Varint, TypeAdapters.VarIntTypeAdapter, true);
            }
        }

        public void InsertLegacySerializer(ColumnTypeCode typeCode, ITypeAdapter typeAdapter, bool reverse)
        {
            var type = typeAdapter.GetDataType();
            var legacySerializer = new LegacyTypeSerializer(typeCode, typeAdapter, reverse);
            _primitiveSerializers[type] = legacySerializer;
            _primitiveDeserializers[typeCode] = legacySerializer;
            _defaultTypes[typeCode] = _ => type;
        }

        /// <summary>
        /// Performs a lightweight validation to determine if the source type and target type matches.
        /// It isn't more strict to support miscellaneous uses of the driver, like direct inputs of blobs and all that. (backward compatibility)
        /// </summary>
        public bool IsAssignableFrom(ColumnTypeCode columnTypeCode, IColumnInfo typeInfo, object value, out string failureMsg)
        {
            failureMsg = null;
            var assignable = IsAssignableHelper(columnTypeCode, typeInfo, value, out var msg);
            if (!assignable)
            {
                failureMsg = msg;
            }

            return assignable;
        }

        private bool IsAssignableHelper(ColumnTypeCode columnTypeCode, IColumnInfo typeInfo, object value, out string failureMsg)
        {
            failureMsg = $"It is not possible to encode a value of type {value?.GetType()} to a CQL type {columnTypeCode}";
            if (value == null || value is byte[])
            {
                return true;
            }

            var type = value.GetType();
            if (_primitiveSerializers.TryGetValue(type, out ITypeSerializer typeSerializer))
            {
                var cqlType = typeSerializer.CqlType;
                //Its a single type, if the types match -> go ahead
                if (cqlType == columnTypeCode)
                {
                    return true;
                }
                //Only int32 and blobs are valid cql ints
                if (columnTypeCode == ColumnTypeCode.Int)
                {
                    return false;
                }
                //Only double, longs and blobs are valid cql double
                if (columnTypeCode == ColumnTypeCode.Double && !(value is long))
                {
                    return false;
                }
                //The rest of the single values are not evaluated
                return true;
            }
            if (columnTypeCode == ColumnTypeCode.List || columnTypeCode == ColumnTypeCode.Set)
            {
                return value is IEnumerable;
            }
            if (columnTypeCode == ColumnTypeCode.Map)
            {
                return value is IDictionary;
            }
            if (columnTypeCode == ColumnTypeCode.Tuple)
            {
                return value is IStructuralComparable;
            }

            if (typeInfo is VectorColumnInfo vectorColumnInfo)
            {
                failureMsg = $"It is not possible to encode a value of type {value?.GetType()} to a vector column, please use the CqlVector<T> type.";
                if (!type.IsGenericType || type.GetGenericTypeDefinition() != typeof(CqlVector<>))
                {
                    return false;
                }

                var vector = (IInternalCqlVector)value;
                if (vector.Count != vectorColumnInfo.Dimension)
                {
                    failureMsg = $"It is not possible to encode a vector of type {value?.GetType()} with count {vector.Count} to a vector column with dimension {vectorColumnInfo.Dimension}";
                    return false;
                }
            }
            return true;
        }

        public UdtMap GetUdtMapByName(string name)
        {
            return _udtSerializer.GetUdtMap(name);
        }

        public UdtMap GetUdtMapByType(Type type)
        {
            return _udtSerializer.GetUdtMap(type);
        }

        public byte[] Serialize(ProtocolVersion version, object value)
        {
            if (value == Unset.Value)
            {
                if (!version.SupportsUnset())
                {
                    throw new InvalidTypeException("Unset is not supported by this Cassandra version");
                }
                //Return a buffer that represents the value Unset
                return SerializerManager.UnsetBuffer;
            }
            if (value == null)
            {
                return null;
            }
            var type = value.GetType();
            if (_primitiveSerializers.TryGetValue(type, out ITypeSerializer typeSerializer))
            {
                return typeSerializer.Serialize((byte)version, value);
            }
            if (_customSerializers.Count > 0 && _customSerializers.TryGetValue(type, out typeSerializer))
            {
                return typeSerializer.Serialize((byte)version, value);
            }
            if (typeof(IEnumerable).GetTypeInfo().IsAssignableFrom(type))
            {
                if (typeof(IDictionary).GetTypeInfo().IsAssignableFrom(type))
                {
                    return _dictionarySerializer.Serialize((byte)version, (IDictionary)value);
                }

                if (typeof(IInternalCqlVector).GetTypeInfo().IsAssignableFrom(type))
                {
                    return _vectorSerializer.Serialize((byte)version, (IInternalCqlVector)value);
                }
                return _collectionSerializer.Serialize((byte)version, (IEnumerable)value);
            }
            if (Utils.IsTuple(type))
            {
                return _tupleSerializer.Serialize((byte)version, (IStructuralEquatable) value);
            }
            var buffer = _udtSerializer.Serialize((byte)version, value);
            if (buffer != null)
            {
                return buffer;
            }
            throw new InvalidTypeException("Unknown Cassandra target type for CLR type " + type.FullName);
        }

        /// <summary>
        /// Sets the <see cref="TypeSerializer{T}"/> instance to use overriding the default ones.
        /// </summary>
        public void SetSpecificSerializers(IEnumerable<ITypeSerializer> typeSerializers)
        {
            if (typeSerializers == null)
            {
                typeSerializers = new List<ITypeSerializer>(0);
            }
            
            var defined = new HashSet<ColumnTypeCode>();
            foreach (var ts in typeSerializers)
            {
                if (defined.Contains(ts.CqlType))
                {
                    //a serializer for a type that was already defined
                    //log a warning and ignore it.
                    Logger.Warning($"A TypeSerializer for {ts.CqlType} has already been defined, ignoring {ts.GetType().Name}");
                    continue;
                }
                if (ts.CqlType == ColumnTypeCode.Custom)
                {
                    _customDeserializers[ts.TypeInfo] = ts;
                    _customSerializers[ts.Type] = ts;
                    Logger.Info($"Using {ts.GetType().Name} serializer for custom type '{((CustomColumnInfo)ts.TypeInfo).CustomTypeName}'");
                    continue;
                }
                //Only one per CQL type, except for Custom.
                defined.Add(ts.CqlType);
                if (ts.CqlType == ColumnTypeCode.Udt)
                {
                    _udtSerializer = (UdtSerializer)ts;
                    Logger.Info($"Using {ts.GetType().Name} serializer for UDT types");
                    continue;
                }
                if (_primitiveDeserializers.ContainsKey(ts.CqlType))
                {
                    Logger.Info($"Using {ts.GetType().Name} serializer for primitive type {ts.CqlType}");
                    _primitiveDeserializers[ts.CqlType] = ts;
                    _primitiveSerializers[ts.Type] = ts;
                    continue;
                }
                throw new DriverInternalError($"TypeSerializer defined for unsupported CQL type {ts.CqlType}");
            }

            // add default custom serializers
            foreach (var defaultCustomSerializer in _defaultCustomTypeSerializers)
            {
                if (defaultCustomSerializer.CqlType != ColumnTypeCode.Custom)
                {
                    throw new DriverInternalError("Expected custom type serializers only.");
                }
                
                if (_customDeserializers.ContainsKey(defaultCustomSerializer.TypeInfo)
                    || _customSerializers.ContainsKey(defaultCustomSerializer.Type))
                {
                    // user overrode default serializer, not logging anything because
                    // it would be logged twice due to SerializerManager.Default and we also
                    // don't log anything for the other default serializers
                    continue;
                }

                _customDeserializers[defaultCustomSerializer.TypeInfo] = defaultCustomSerializer;
                _customSerializers[defaultCustomSerializer.Type] = defaultCustomSerializer;
            }
        }

        /// <summary>
        /// Adds a UDT mapping definition
        /// </summary>
        public void SetUdtMap(string name, UdtMap map)
        {
            _udtSerializer.SetUdtMap(name, map);
        }

        internal static void ValidateColumnInfo(ColumnTypeCode typeCode, IColumnInfo typeInfo)
        {
            string msg = null;
            bool requiresCheck = false;
            var typeCodeName = Enum.GetName(typeof(ColumnTypeCode), typeCode);
            switch (typeCode)
            {
                case ColumnTypeCode.List:
                case ColumnTypeCode.Set:
                    requiresCheck = true;
                    if (!(typeInfo is ListColumnInfo) && !(typeInfo is SetColumnInfo))
                    {
                        msg =
                            "list and set column types require a column info object of type ListColumnInfo or SetColumnInfo.";
                    }

                    break;
                case ColumnTypeCode.Map:
                    requiresCheck = true;
                    if (!(typeInfo is MapColumnInfo))
                    {
                        msg = "map column type requires a column info object of type MapColumnInfo.";
                    }

                    break;
                case ColumnTypeCode.Custom:
                    requiresCheck = true;
                    if (!(typeInfo is CustomColumnInfo) && !(typeInfo is VectorColumnInfo))
                    {
                        msg = "custom column type requires a column info object of type CustomColumnInfo.";
                    }

                    break;
                case ColumnTypeCode.Udt:
                    requiresCheck = true;
                    if (!(typeInfo is UdtColumnInfo))
                    {
                        msg = "udt column type requires a column info object of type UdtColumnInfo.";
                    }

                    break;
                case ColumnTypeCode.Tuple:
                    requiresCheck = true;
                    if (!(typeInfo is TupleColumnInfo))
                    {
                        msg = "tuple column type requires a column info object of type TupleColumnInfo.";
                    }

                    break;
            }

            if (requiresCheck && typeInfo == null)
            {
                throw new ArgumentException($"Cql type {typeCodeName} requires a type info object (e.g. list requires " +
                                            "an instance of ListColumnInfo to be provided).");
            }

            if (msg != null)
            {
                throw new ArgumentException(msg + " An instance of " + typeInfo.GetType().AssemblyQualifiedName + " was provided instead.");
            }
        }

        public int GetValueLengthIfFixed(ColumnTypeCode typeCode, IColumnInfo typeInfo)
        {
            switch (typeCode)
            {
                case ColumnTypeCode.Bigint:
                    return 8;
                case ColumnTypeCode.Boolean:
                    return 1;
                case ColumnTypeCode.Timestamp:
                    return 8;
                case ColumnTypeCode.Double:
                    return 8;
                case ColumnTypeCode.Float:
                    return 4;
                case ColumnTypeCode.Int:
                    return 4;
                case ColumnTypeCode.Timeuuid:
                    return 16;
                case ColumnTypeCode.Uuid:
                    return 16;

                case ColumnTypeCode.Ascii:
                case ColumnTypeCode.TinyInt:
                case ColumnTypeCode.Blob:
                case ColumnTypeCode.Counter:
                case ColumnTypeCode.Decimal:
                case ColumnTypeCode.Duration:
                case ColumnTypeCode.Inet:
                case ColumnTypeCode.Varint:
                case ColumnTypeCode.SmallInt:
                case ColumnTypeCode.Date:
                case ColumnTypeCode.Time:
                case ColumnTypeCode.Text:
                case ColumnTypeCode.Varchar:
                case ColumnTypeCode.List:
                case ColumnTypeCode.Map:
                case ColumnTypeCode.Set:
                case ColumnTypeCode.Udt:
                case ColumnTypeCode.Tuple:
                    return -1;

                case ColumnTypeCode.Custom:
                    if (typeInfo is VectorColumnInfo vectorTypeInfo)
                    {
                        var subTypeValueLength = GetValueLengthIfFixed(vectorTypeInfo.ValueTypeCode, vectorTypeInfo.ValueTypeInfo);
                        if (subTypeValueLength < 0)
                        {
                            return -1;
                        }

                        if (!vectorTypeInfo.Dimension.HasValue)
                        {
                            throw new DriverInternalError("Can not compute vector value length without the dimension.");
                        }

                        return subTypeValueLength * vectorTypeInfo.Dimension.Value;
                    }

                    return -1;
            }

            throw new DriverInternalError($"could not determine value length for type {typeCode}");
        }

        public int GetValueLengthIfFixed(object value)
        {
            if (value == null)
            {
                throw new ArgumentNullException(nameof(value));
            }
            var type = value.GetType();
            if (_primitiveSerializers.TryGetValue(type, out ITypeSerializer typeSerializer))
            {
                return GetValueLengthIfFixed(typeSerializer.CqlType, typeSerializer.TypeInfo);
            }
            if (_customSerializers.Count > 0 && _customSerializers.TryGetValue(type, out typeSerializer))
            {
                return GetValueLengthIfFixed(typeSerializer.CqlType, typeSerializer.TypeInfo);
            }
            if (type.IsArray)
            {
                return -1;
            }
            if (type.GetTypeInfo().IsGenericType)
            {
                var genericTypeDefinition = type.GetGenericTypeDefinition();

                if (genericTypeDefinition == typeof(CqlVector<>))
                {
                    var vector = (IInternalCqlVector)value;
                    if (vector.Count <= 0)
                    {
                        throw new ArgumentException("vector dimension has to be greater than 0");
                    }

                    var subTypeValueLength = GetValueLengthIfFixed(vector[0]);
                    if (subTypeValueLength < 0)
                    {
                        return -1;
                    }

                    return subTypeValueLength * vector.Count;
                }
                if (typeof(IEnumerable).GetTypeInfo().IsAssignableFrom(type))
                {
                    return -1;
                }
                if (typeof(IStructuralComparable).GetTypeInfo().IsAssignableFrom(type) && type.FullName.StartsWith("System.Tuple"))
                {
                    return -1;
                }
            }

            //Determine if its a Udt type
            var udtMap = _udtSerializer.GetUdtMap(type);
            if (udtMap != null)
            {
                return -1;
            }
            throw new DriverInternalError($"could not determine value length for type {type.FullName}");
        }
    }
}