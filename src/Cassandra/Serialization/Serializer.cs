﻿//
//      Copyright (C) 2012-2016 DataStax Inc.
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
using System.Collections;
using System.Collections.Generic;
using System.Linq;
using System.Numerics;

namespace Cassandra.Serialization
{
    /// <summary>
    /// Handles types serialization from binary form to objects and the other way around.
    /// The instance is aware of protocol version, custom codecs, UDT mappers
    /// </summary>
    internal class Serializer
    {
        private static readonly Logger Logger = new Logger(typeof (Serializer));
        internal static readonly Serializer Default = new Serializer(1);
        private volatile byte _protocolVersion;
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
            { ColumnTypeCode.Float, TypeSerializer.PrimitiveFloatSerializer },
            { ColumnTypeCode.Inet, TypeSerializer.PrimitiveIpAddressSerializer },
            { ColumnTypeCode.Int, TypeSerializer.PrimitiveIntSerializer },
            { ColumnTypeCode.SmallInt, TypeSerializer.PrimitiveShortSerializer },
            { ColumnTypeCode.Text, TypeSerializer.PrimitiveStringSerializer },
            { ColumnTypeCode.Time, TypeSerializer.PrimitiveLocalTimeSerializer},
            { ColumnTypeCode.Timestamp, TypeSerializer.PrimitiveDateTimeOffsetSerializer },
            { ColumnTypeCode.Timeuuid, TypeSerializer.PrimitiveTimeUuidSerializer },
            { ColumnTypeCode.TinyInt, TypeSerializer.PrimitiveSbyteSerializer },
            { ColumnTypeCode.Uuid, TypeSerializer.PrimitiveGuidSerializer },
            { ColumnTypeCode.Varchar, TypeSerializer.PrimitiveStringSerializer },
            { ColumnTypeCode.Varint, TypeSerializer.PrimitiveBigIntegerSerializer }
        };

        private readonly Dictionary<Type, ITypeSerializer> _primitiveSerializers = new Dictionary<Type, ITypeSerializer>();
        private readonly IDictionary<IColumnInfo, ITypeSerializer> _customDeserializers = new Dictionary<IColumnInfo, ITypeSerializer>();
        private readonly IDictionary<Type, ITypeSerializer> _customSerializers = new Dictionary<Type, ITypeSerializer>();
        private readonly CollectionSerializer _collectionSerializer = new CollectionSerializer();
        private readonly DictionarySerializer _dictionarySerializer = new DictionarySerializer();
        private readonly TupleSerializer _tupleSerializer = new TupleSerializer();
        private readonly Dictionary<ColumnTypeCode, Func<IColumnInfo, Type>> _defaultTypes = new Dictionary<ColumnTypeCode,Func<IColumnInfo,Type>>();
        //Udt serializer can be specified
        private UdtSerializer _udtSerializer = new UdtSerializer();


        /// <summary>
        /// An instance of a buffer that represents the value Unset
        /// </summary>
        internal static readonly byte[] UnsetBuffer = new byte[0];

        internal byte ProtocolVersion
        {
            get { return _protocolVersion; }
            set { _protocolVersion = value; }
        }

        internal Serializer(byte protocolVersion, IEnumerable<ITypeSerializer> typeSerializers = null)
        {
            _protocolVersion = protocolVersion;
            InitPrimitiveSerializers();
            _collectionSerializer.SetChildSerializer(this);
            _dictionarySerializer.SetChildSerializer(this);
            _tupleSerializer.SetChildSerializer(this);
            _udtSerializer.SetChildSerializer(this);
            InitDefaultTypes();
            InitTypeAdapters();
            SetSpecificSerializers(typeSerializers);
        }

        internal object Deserialize(byte[] buffer, int offset, int length, ColumnTypeCode typeCode, IColumnInfo typeInfo)
        {
            ITypeSerializer typeSerializer;
            if (_primitiveDeserializers.TryGetValue(typeCode, out typeSerializer))
            {
                return typeSerializer.Deserialize(_protocolVersion, buffer, offset, length, typeInfo);
            }
            switch (typeCode)
            {
                case ColumnTypeCode.Custom:
                {
                    if (_customDeserializers.Count == 0 || !_customDeserializers.TryGetValue(typeInfo, out typeSerializer))
                    {
                        // Use byte[] by default
                        typeSerializer = TypeSerializer.PrimitiveByteArraySerializer;
                    }
                    return typeSerializer.Deserialize(_protocolVersion, buffer, offset, length, typeInfo);
                }
                case ColumnTypeCode.Udt:
                    return _udtSerializer.Deserialize(_protocolVersion, buffer, offset, length, typeInfo);
                case ColumnTypeCode.List:
                case ColumnTypeCode.Set:
                    return _collectionSerializer.Deserialize(_protocolVersion, buffer, offset, length, typeInfo);
                case ColumnTypeCode.Map:
                    return _dictionarySerializer.Deserialize(_protocolVersion, buffer, offset, length, typeInfo);
                case ColumnTypeCode.Tuple:
                    return _tupleSerializer.Deserialize(_protocolVersion, buffer, offset, length, typeInfo);
            }
            //Unknown type, return the byte representation
            return buffer;
        }

        internal Type GetClrType(ColumnTypeCode typeCode, IColumnInfo typeInfo)
        {
            Func<IColumnInfo, Type> clrTypeHandler;
            if (!_defaultTypes.TryGetValue(typeCode, out clrTypeHandler))
            {
                throw new ArgumentException("No handler defined for type " + typeCode);
            }
            return clrTypeHandler(typeInfo);
        }

        private Type GetClrTypeForCustom(IColumnInfo typeInfo)
        {
            var customTypeInfo = (CustomColumnInfo)typeInfo;
            if (customTypeInfo.CustomTypeName == null || !customTypeInfo.CustomTypeName.StartsWith(DataTypeParser.UdtTypeName))
            {
                ITypeSerializer serializer;
                if (_customDeserializers.TryGetValue(customTypeInfo, out serializer))
                {
                    return serializer.Type;
                }
                return TypeSerializer.PrimitiveByteArraySerializer.Type;
            }
            var dataType = DataTypeParser.ParseFqTypeName(customTypeInfo.CustomTypeName);
            var map = _udtSerializer.GetUdtMap(dataType.Keyspace + "." + dataType.Name);
            if (map == null)
            {
                throw new InvalidTypeException("No mapping defined for udt type " + dataType.Keyspace + "." + dataType.Name);
            }
            return map.NetType;
        }

        public ColumnTypeCode GetCqlType(Type type, out IColumnInfo typeInfo)
        {
            typeInfo = null;
            ITypeSerializer typeSerializer;
            if (_primitiveSerializers.TryGetValue(type, out typeSerializer))
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
                IColumnInfo valueTypeInfo;
                ColumnTypeCode valueTypeCode = GetCqlType(type.GetElementType(), out valueTypeInfo);
                typeInfo = new ListColumnInfo
                {
                    ValueTypeCode = valueTypeCode,
                    ValueTypeInfo = valueTypeInfo
                };
                return ColumnTypeCode.List;
            }
            if (type.IsGenericType)
            {
                if (type.GetGenericTypeDefinition() == typeof(Nullable<>))
                {
                    return GetCqlType(type.GetGenericArguments()[0], out typeInfo);
                }
                if (typeof(IEnumerable).IsAssignableFrom(type))
                {
                    IColumnInfo valueTypeInfo;
                    ColumnTypeCode valueTypeCode;
                    var interfaces = type.GetInterfaces();
                    if (typeof(IDictionary).IsAssignableFrom(type) && 
                        interfaces.Any(t => t.IsGenericType && t.GetGenericTypeDefinition() == typeof(IDictionary<,>)))
                    {
                        IColumnInfo keyTypeInfo;
                        var keyTypeCode = GetCqlType(type.GetGenericArguments()[0], out keyTypeInfo);
                        valueTypeCode = GetCqlType(type.GetGenericArguments()[1], out valueTypeInfo);
                        typeInfo = new MapColumnInfo
                        {
                            KeyTypeCode = keyTypeCode,
                            KeyTypeInfo = keyTypeInfo,
                            ValueTypeCode = valueTypeCode,
                            ValueTypeInfo = valueTypeInfo
                        };
                        return ColumnTypeCode.Map;
                    }
                    if (interfaces.Any(t => t.IsGenericType && t.GetGenericTypeDefinition() == typeof(ISet<>)))
                    {
                        IColumnInfo keyTypeInfo;
                        var keyTypeCode = GetCqlType(type.GetGenericArguments()[0], out keyTypeInfo);
                        typeInfo = new SetColumnInfo
                        {
                            KeyTypeCode = keyTypeCode,
                            KeyTypeInfo = keyTypeInfo
                        };
                        return ColumnTypeCode.Set;
                    }
                    valueTypeCode = GetCqlType(type.GetGenericArguments()[0], out valueTypeInfo);
                    typeInfo = new ListColumnInfo
                    {
                        ValueTypeCode = valueTypeCode,
                        ValueTypeInfo = valueTypeInfo
                    };
                    return ColumnTypeCode.List;
                }
                if (typeof(IStructuralComparable).IsAssignableFrom(type) && type.FullName.StartsWith("System.Tuple"))
                {
                    typeInfo = new TupleColumnInfo
                    {
                        Elements = type.GetGenericArguments().Select(t =>
                        {
                            IColumnInfo tupleSubTypeInfo;
                            var dataType = new ColumnDesc
                            {
                                TypeCode = GetCqlType(t, out tupleSubTypeInfo),
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
            throw new InvalidTypeException("Unknown Cassandra target type for CLR type " + type.FullName);
        }

        private void InitPrimitiveSerializers()
        {
            //Default primitive serializers
            foreach (var serializer in _primitiveDeserializers.Values)
            {
                _primitiveSerializers[serializer.Type] = serializer;
            }
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

        private void InsertLegacySerializer(ColumnTypeCode typeCode, ITypeAdapter typeAdapter, bool reverse)
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
        public bool IsAssignableFrom(CqlColumn column, object value)
        {
            if (value == null || value is byte[])
            {
                return true;
            }
            var type = value.GetType();
            ITypeSerializer typeSerializer;
            if (_primitiveSerializers.TryGetValue(type, out typeSerializer))
            {
                var cqlType = typeSerializer.CqlType;
                //Its a single type, if the types match -> go ahead
                if (cqlType == column.TypeCode)
                {
                    return true;
                }
                //Only int32 and blobs are valid cql ints
                if (column.TypeCode == ColumnTypeCode.Int)
                {
                    return false;
                }
                //Only double, longs and blobs are valid cql double
                if (column.TypeCode == ColumnTypeCode.Double && !(value is long))
                {
                    return false;
                }
                //The rest of the single values are not evaluated
                return true;
            }
            if (column.TypeCode == ColumnTypeCode.List || column.TypeCode == ColumnTypeCode.Set)
            {
                return value is IEnumerable;
            }
            if (column.TypeCode == ColumnTypeCode.Map)
            {
                return value is IDictionary;
            }
            if (column.TypeCode == ColumnTypeCode.Tuple)
            {
                return value is IStructuralComparable;
            }
            return true;
        }

        internal byte[] Serialize(object value)
        {
            if (value == Unset.Value)
            {
                if (_protocolVersion < 4)
                {
                    throw new InvalidTypeException("Unset is not supported by this Cassandra version");
                }
                //Return a buffer that represents the value Unset
                return UnsetBuffer;
            }
            if (value == null)
            {
                return null;
            }
            var type = value.GetType();
            ITypeSerializer typeSerializer;
            if (_primitiveSerializers.TryGetValue(type, out typeSerializer))
            {
                return typeSerializer.Serialize(_protocolVersion, value);
            }
            if (_customSerializers.Count > 0 && _customSerializers.TryGetValue(type, out typeSerializer))
            {
                return typeSerializer.Serialize(_protocolVersion, value);
            }
            if (typeof(IEnumerable).IsAssignableFrom(type))
            {
                if (typeof(IDictionary).IsAssignableFrom(type))
                {
                    return _dictionarySerializer.Serialize(_protocolVersion, (IDictionary)value);
                }
                return _collectionSerializer.Serialize(_protocolVersion, (IEnumerable)value);
            }
            if (typeof(IStructuralComparable).IsAssignableFrom(type) && type.FullName.StartsWith("System.Tuple"))
            {
                return _tupleSerializer.Serialize(_protocolVersion, (IStructuralEquatable) value);
            }
            var buffer = _udtSerializer.Serialize(_protocolVersion, value);
            if (buffer != null)
            {
                return buffer;
            }
            throw new InvalidTypeException("Unknown Cassandra target type for CLR type " + type.FullName);
        }

        /// <summary>
        /// Sets the <see cref="TypeSerializer{T}"/> instance to use overriding the default ones.
        /// </summary>
        private void SetSpecificSerializers(IEnumerable<ITypeSerializer> typeSerializers)
        {
            if (typeSerializers == null)
            {
                return;
            }
            var defined = new HashSet<ColumnTypeCode>();
            foreach (var ts in typeSerializers)
            {
                if (defined.Contains(ts.CqlType))
                {
                    //a serializer for a type that was already defined
                    //log a warning and ignore it.
                    Logger.Warning("A TypeSerializer for {0} has already been defined, ignoring {1}", ts.CqlType, ts.GetType().Name);
                    continue;
                }
                if (ts.CqlType == ColumnTypeCode.Custom)
                {
                    _customDeserializers[ts.TypeInfo] = ts;
                    _customSerializers[ts.Type] = ts;
                    Logger.Info("Using {0} serializer for custom type '{1}'", ts.GetType().Name, ((CustomColumnInfo)ts.TypeInfo).CustomTypeName);
                    continue;
                }
                //Only one per CQL type, except for Custom.
                defined.Add(ts.CqlType);
                if (ts.CqlType == ColumnTypeCode.Udt)
                {
                    _udtSerializer = (UdtSerializer)ts;
                    Logger.Info("Using {0} serializer for UDT types", ts.GetType().Name);
                    continue;
                }
                if (_primitiveDeserializers.ContainsKey(ts.CqlType))
                {
                    Logger.Info("Using {0} serializer for primitive type {1}", ts.GetType().Name, ts.CqlType);
                    _primitiveDeserializers[ts.CqlType] = ts;
                    _primitiveSerializers[ts.Type] = ts;
                    continue;
                }
                throw new DriverInternalError("TypeSerializer defined for unsupported CQL type " + ts.CqlType);
            }
        }

        /// <summary>
        /// Adds a UDT mapping definition
        /// </summary>
        internal void SetUdtMap(string name, UdtMap map)
        {
            _udtSerializer.SetUdtMap(name, map);
        }
    }
}
