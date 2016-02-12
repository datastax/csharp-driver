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
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Net;

namespace Cassandra.Serialization
{
    /// <summary>
    /// Handles types serialization from binary form to objects and the other way around.
    /// The instance is aware of protocol version, custom codecs, UDT mappers
    /// </summary>
    internal class Serializer
    {
        private const string ListTypeName = "org.apache.cassandra.db.marshal.ListType";
        private const string SetTypeName = "org.apache.cassandra.db.marshal.SetType";
        private const string MapTypeName = "org.apache.cassandra.db.marshal.MapType";
        private const string UdtTypeName = "org.apache.cassandra.db.marshal.UserType";
        private const string TupleTypeName = "org.apache.cassandra.db.marshal.TupleType";
        private const string FrozenTypeName = "org.apache.cassandra.db.marshal.FrozenType";
        public const string ReversedTypeName = "org.apache.cassandra.db.marshal.ReversedType";
        public const string CompositeTypeName = "org.apache.cassandra.db.marshal.CompositeType";
        private const string EmptyTypeName = "org.apache.cassandra.db.marshal.EmptyType";

        /// <summary>
        /// Contains the cql literals of certain types
        /// </summary>
        private static class CqlNames
        {
            public const string Frozen = "frozen";
            public const string List = "list";
            public const string Set = "set";
            public const string Map = "map";
            public const string Tuple = "tuple";
            public const string Empty = "empty";
        }
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
        private readonly Dictionary<ColumnTypeCode, Func<IColumnInfo, Type>> _defaultTypes = new Dictionary<ColumnTypeCode,Func<IColumnInfo,Type>>();


        /// <summary>
        /// An instance of a buffer that represents the value Unset
        /// </summary>
        internal static readonly byte[] UnsetBuffer = new byte[0];

        internal Serializer(ushort protocolVersion)
        {
            _protocolVersion = protocolVersion;
            _udtSerializer.SetChildSerializer(this);
            _collectionSerializer.SetChildSerializer(this);
            InitPrimitiveSerializers();
            InitDefaultTypes();
            InitTypeAdapters();
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
            _primitiveSerializers.Add(typeof(DateTime), TypeSerializer.PrimitiveLocalDateSerializer);
        }

        private void InitDefaultTypes()
        {
            foreach (var kv in _primitiveDeserializers)
            {
                var clrType = kv.Value.Type;
                _defaultTypes.Add(kv.Key, _ => clrType);
            }
            _defaultTypes.Add(ColumnTypeCode.Custom, GetClrTypeForCustom);
            _defaultTypes.Add(ColumnTypeCode.List, GetClrTypeForList);
            _defaultTypes.Add(ColumnTypeCode.Set, GetClrTypeForSet);
            _defaultTypes.Add(ColumnTypeCode.Map, GetClrTypeForMap);
            _defaultTypes.Add(ColumnTypeCode.Udt, GetClrTypeForUdt);
            _defaultTypes.Add(ColumnTypeCode.Tuple, GetClrTypeForTuple);
        }

        private void InitTypeAdapters()
        {
            //TypeAdapters was the way we exposed type encoding/decoding extensions since v1
            //Its going to be removed in future versions but we have to support them for now.
            if (_primitiveDeserializers[ColumnTypeCode.Decimal].Type != TypeAdapters.DecimalTypeAdapter.GetDataType())
            {
                //TODO: Add to default types ??
                //TODO: Add to serializers
            }
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
            var customTypeInfo = ValidateTypeInfo<CustomColumnInfo>(typeInfo);
            if (customTypeInfo.CustomTypeName == null || !customTypeInfo.CustomTypeName.StartsWith(UdtTypeName))
            {
                ITypeSerializer serializer;
                if (_customDeserializers.TryGetValue(customTypeInfo, out serializer))
                {
                    return serializer.Type;
                }
                return TypeSerializer.PrimitiveByteArraySerializer.Type;
            }
            //Check UDTs under protocol v3
            throw new NotImplementedException();
        }

        private Type GetClrTypeForList(IColumnInfo typeInfo)
        {
            var listTypeInfo = ValidateTypeInfo<ListColumnInfo>(typeInfo);
            var valueType = GetClrType(listTypeInfo.ValueTypeCode, listTypeInfo.ValueTypeInfo);
            var openType = typeof(IEnumerable<>);
            return openType.MakeGenericType(valueType);
        }

        private Type GetClrTypeForMap(IColumnInfo typeInfo)
        {
            var mapTypeInfo = ValidateTypeInfo<MapColumnInfo>(typeInfo);
            var keyType = GetClrType(mapTypeInfo.KeyTypeCode, mapTypeInfo.KeyTypeInfo);
            var valueType = GetClrType(mapTypeInfo.ValueTypeCode, mapTypeInfo.ValueTypeInfo);
            var openType = typeof(IDictionary<,>);
            return openType.MakeGenericType(keyType, valueType);
        }

        private Type GetClrTypeForSet(IColumnInfo typeInfo)
        {
            var listTypeInfo = ValidateTypeInfo<SetColumnInfo>(typeInfo);
            var valueType = GetClrType(listTypeInfo.KeyTypeCode, listTypeInfo.KeyTypeInfo);
            var openType = typeof(IEnumerable<>);
            return openType.MakeGenericType(valueType);
        }

        private Type GetClrTypeForTuple(IColumnInfo typeInfo)
        {
            var tupleInfo = ValidateTypeInfo<TupleColumnInfo>(typeInfo);
            Type genericTupleType;
            switch (tupleInfo.Elements.Count)
            {
                case 1:
                    genericTupleType = typeof(Tuple<>);
                    break;
                case 2:
                    genericTupleType = typeof(Tuple<,>);
                    break;
                case 3:
                    genericTupleType = typeof(Tuple<,,>);
                    break;
                case 4:
                    genericTupleType = typeof(Tuple<,,,>);
                    break;
                case 5:
                    genericTupleType = typeof(Tuple<,,,,>);
                    break;
                case 6:
                    genericTupleType = typeof(Tuple<,,,,,>);
                    break;
                case 7:
                    genericTupleType = typeof(Tuple<,,,,,,>);
                    break;
                default:
                    return typeof(byte[]);
            }
            return genericTupleType.MakeGenericType(
                tupleInfo.Elements.Select(s => GetClrType(s.TypeCode, s.TypeInfo)).ToArray());
        }

        private Type GetClrTypeForUdt(IColumnInfo typeInfo)
        {
            var udtInfo = ValidateTypeInfo<UdtColumnInfo>(typeInfo);
            var map = _udtSerializer.GetUdtMap(udtInfo);
            return map == null ? typeof (byte[]) : map.NetType;
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
            //TODO: Collections, maps and tuple
            //Subchilds encoder instance
            var buffer = _udtSerializer.Serialize(_protocolVersion, value);
            if (buffer != null)
            {
                return buffer;
            }
            throw new InvalidTypeException("Unknown Cassandra target type for CLR type " + type.FullName);
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
                _udtSerializer.Deserialize(_protocolVersion, buffer, typeInfo);
            }
            //TODO: Collections, udt and tuple
            throw new NotImplementedException();
        }

        private static T ValidateTypeInfo<T>(IColumnInfo typeInfo)
        {
            if (typeInfo == null)
            {
                throw new ArgumentNullException("typeInfo");
            }
            if (!(typeInfo is T))
            {
                throw new ArgumentException(string.Format("Expected {0} typeInfo, obtained {1}", typeof(T), typeInfo.GetType()));
            }
            return (T)typeInfo;
        }
    }
}
