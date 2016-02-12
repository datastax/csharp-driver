//
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
        private volatile ushort _protocolVersion;
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
            { ColumnTypeCode.Timeuuid, TypeSerializer.PrimitiveGuidSerializer },
            { ColumnTypeCode.TinyInt, TypeSerializer.PrimitiveSbyteSerializer },
            { ColumnTypeCode.Uuid, TypeSerializer.PrimitiveGuidSerializer },
            { ColumnTypeCode.Varchar, TypeSerializer.PrimitiveStringSerializer },
            { ColumnTypeCode.Varint, TypeSerializer.PrimitiveBigIntegerSerializer }
        };

        private readonly Dictionary<Type, ITypeSerializer> _primitiveSerializers = new Dictionary<Type, ITypeSerializer>();
        private readonly IDictionary<IColumnInfo, ITypeSerializer> _customDeserializers = new Dictionary<IColumnInfo, ITypeSerializer>();
        private readonly IDictionary<Type, ITypeSerializer> _customSerializers = new Dictionary<Type, ITypeSerializer>();
        private readonly UdtSerializer _udtSerializer = new UdtSerializer();
        private readonly CollectionSerializer _collectionSerializer = new CollectionSerializer();
        private readonly DictionarySerializer _dictionarySerializer = new DictionarySerializer();
        private readonly TupleSerializer _tupleSerializer = new TupleSerializer();
        private readonly Dictionary<ColumnTypeCode, Func<IColumnInfo, Type>> _defaultTypes = new Dictionary<ColumnTypeCode,Func<IColumnInfo,Type>>();


        /// <summary>
        /// An instance of a buffer that represents the value Unset
        /// </summary>
        internal static readonly byte[] UnsetBuffer = new byte[0];

        internal Serializer(ushort protocolVersion)
        {
            _protocolVersion = protocolVersion;
            InitPrimitiveSerializers();
            _collectionSerializer.SetChildSerializer(this);
            _dictionarySerializer.SetChildSerializer(this);
            _tupleSerializer.SetChildSerializer(this);
            _udtSerializer.SetChildSerializer(this);
            InitDefaultTypes();
            InitTypeAdapters();
        }

        internal object Deserialize(byte[] buffer, ColumnTypeCode typeCode, IColumnInfo typeInfo)
        {
            ITypeSerializer typeSerializer;
            if (_primitiveDeserializers.TryGetValue(typeCode, out typeSerializer))
            {
                return typeSerializer.Deserialize(_protocolVersion, buffer, typeInfo);
            }
            if (typeCode == ColumnTypeCode.Custom)
            {
                if (_customDeserializers.Count == 0 || !_customDeserializers.TryGetValue(typeInfo, out typeSerializer))
                {
                    // Use byte[] by default
                    typeSerializer = TypeSerializer.PrimitiveByteArraySerializer;
                }
                return typeSerializer.Deserialize(_protocolVersion, buffer, typeInfo);
            }
            if (typeCode == ColumnTypeCode.Udt)
            {
                return _udtSerializer.Deserialize(_protocolVersion, buffer, typeInfo);
            }
            if (typeCode == ColumnTypeCode.List || typeCode == ColumnTypeCode.Set)
            {
                return _collectionSerializer.Deserialize(_protocolVersion, buffer, typeInfo);
            }
            if (typeCode == ColumnTypeCode.Map)
            {
                return _dictionarySerializer.Deserialize(_protocolVersion, buffer, typeInfo);
            }
            if (typeCode == ColumnTypeCode.Tuple)
            {
                return _tupleSerializer.Deserialize(_protocolVersion, buffer, typeInfo);
            }
            if (typeCode == ColumnTypeCode.Udt)
            {
                return _udtSerializer.Deserialize(_protocolVersion, buffer, typeInfo);
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

        internal ColumnTypeCode GetCqlTypeForPrimitive(Type type)
        {
            return _primitiveSerializers[type].CqlType;
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
                InsertLegacySerializer(ColumnTypeCode.Decimal, TypeAdapters.DecimalTypeAdapter);
            }
            if (TypeAdapters.VarIntTypeAdapter.GetDataType() != typeof(BigInteger))
            {
                InsertLegacySerializer(ColumnTypeCode.Varint, TypeAdapters.VarIntTypeAdapter);
            }
        }

        private void InsertLegacySerializer(ColumnTypeCode typeCode, ITypeAdapter typeAdapter)
        {
            var type = typeAdapter.GetDataType();
            var legacySerializer = new LegacyTypeSerializer(typeCode, typeAdapter);
            _primitiveSerializers[type] = legacySerializer;
            _primitiveDeserializers[typeCode] = legacySerializer;
            _defaultTypes[typeCode] = _ => type;
        }

        internal void SetProtocolVersion(ushort protocolVersion)
        {
            _protocolVersion = protocolVersion;
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
    }
}
