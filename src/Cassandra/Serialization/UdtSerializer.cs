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
using System.Reflection;
using System.Collections.Concurrent;
using System.Collections.Generic;

namespace Cassandra.Serialization
{
    /// <summary>
    /// Represents a <see cref="TypeSerializer{T}"/> instance that handles UDT serialization and deserialization.
    /// </summary>
    public class UdtSerializer : TypeSerializer<object>
    {
        private readonly ConcurrentDictionary<string, UdtMap> _udtMapsByName = new ConcurrentDictionary<string, UdtMap>();
        private readonly ConcurrentDictionary<Type, UdtMap> _udtMapsByClrType = new ConcurrentDictionary<Type, UdtMap>();

        public override ColumnTypeCode CqlType => ColumnTypeCode.Udt;

        protected internal UdtSerializer()
        {

        }

        protected internal virtual Type GetClrType(IColumnInfo typeInfo)
        {
            var udtInfo = (UdtColumnInfo)typeInfo;
            var map = GetUdtMap(udtInfo.Name);
            return map == null ? typeof(byte[]) : map.NetType;
        }

        protected internal virtual UdtMap GetUdtMap(string name)
        {
            _udtMapsByName.TryGetValue(name, out UdtMap map);
            return map;
        }

        protected internal virtual UdtMap GetUdtMap(Type type)
        {
            _udtMapsByClrType.TryGetValue(type, out UdtMap map);
            return map;
        }

        public override object Deserialize(ushort protocolVersion, byte[] buffer, int offset, int length, IColumnInfo typeInfo)
        {
            var udtInfo = (UdtColumnInfo)typeInfo;
            var map = GetUdtMap(udtInfo.Name);
            if (map == null)
            {
                return buffer;
            }
            var valuesList = new object[udtInfo.Fields.Count];
            var maxOffset = offset + length;
            for (var i = 0; i < udtInfo.Fields.Count; i++)
            {
                var field = udtInfo.Fields[i];
                if (offset >= maxOffset)
                {
                    break;
                }
                var itemLength = BeConverter.ToInt32(buffer, offset);
                offset += 4;
                if (itemLength < 0)
                {
                    continue;
                }
                valuesList[i] = DeserializeChild(protocolVersion, buffer, offset, itemLength, field.TypeCode, field.TypeInfo);
                offset += itemLength;
            }
            return map.ToObject(valuesList);
        }

        public override byte[] Serialize(ushort protocolVersion, object value)
        {
            if (value == null)
            {
                throw new ArgumentNullException("value");
            }
            var map = GetUdtMap(value.GetType());
            if (map == null)
            {
                return null;
            }
            var bufferList = new List<byte[]>();
            var bufferLength = 0;
            foreach (var field in map.Definition.Fields)
            {
                object fieldValue = null;
                var prop = map.GetPropertyForUdtField(field.Name);
                var fieldTargetType = GetClrType(field.TypeCode, field.TypeInfo);
                if (prop != null)
                {
                    fieldValue = prop.GetValue(value, null);
                    if (!fieldTargetType.GetTypeInfo().IsAssignableFrom(prop.PropertyType.GetTypeInfo()))
                    {
                        fieldValue = UdtMap.TypeConverter.ConvertToDbFromUdtFieldValue(prop.PropertyType,
                                               fieldTargetType,
                                               fieldValue);
                    }
                }
                var itemBuffer = SerializeChild(protocolVersion, fieldValue);
                bufferList.Add(itemBuffer);
                if (fieldValue != null)
                {
                    bufferLength += itemBuffer.Length;
                }
            }
            return EncodeBufferList(bufferList, bufferLength);
        }

        /// <summary>
        /// Sets a Udt map for a given Udt name
        /// </summary>
        /// <param name="name">Fully qualified udt name case sensitive (keyspace.udtName)</param>
        /// <param name="map"></param>
        public virtual void SetUdtMap(string name, UdtMap map)
        {
            _udtMapsByName.AddOrUpdate(name, map, (k, oldValue) => map);
            _udtMapsByClrType.AddOrUpdate(map.NetType, map, (k, oldValue) => map);
        }
    }
}
