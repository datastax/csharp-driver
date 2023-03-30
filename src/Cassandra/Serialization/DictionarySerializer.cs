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
using System.Collections;
using System.Collections.Generic;

namespace Cassandra.Serialization
{
    internal class DictionarySerializer : TypeSerializer<IDictionary>
    {
        public override ColumnTypeCode CqlType
        {
            get { return ColumnTypeCode.Map; }
        }

        public override IDictionary Deserialize(ushort protocolVersion, byte[] buffer, int offset, int length, IColumnInfo typeInfo)
        {
            var mapInfo = (MapColumnInfo) typeInfo;
            var keyType = GetClrType(mapInfo.KeyTypeCode, mapInfo.KeyTypeInfo);
            var valueType = GetClrType(mapInfo.ValueTypeCode, mapInfo.ValueTypeInfo);
            var count = DecodeCollectionLength((ProtocolVersion)protocolVersion, buffer, ref offset);
            var dicType = typeof(SortedDictionary<,>).MakeGenericType(keyType, valueType);
            var result = (IDictionary)Activator.CreateInstance(dicType);
            for (var i = 0; i < count; i++)
            {
                var keyLength = DecodeCollectionLength((ProtocolVersion)protocolVersion, buffer, ref offset);
                var key = DeserializeChild(protocolVersion, buffer, offset, keyLength, mapInfo.KeyTypeCode, mapInfo.KeyTypeInfo);
                offset += keyLength;

                var valueLength = DecodeCollectionLength((ProtocolVersion)protocolVersion, buffer, ref offset);
                var value = DeserializeChild(protocolVersion, buffer, offset, valueLength, mapInfo.ValueTypeCode, mapInfo.ValueTypeInfo);
                offset += valueLength;

                result.Add(key, value);
            }
            return result;
        }

        internal Type GetClrType(IColumnInfo typeInfo)
        {
            var mapTypeInfo = (MapColumnInfo)typeInfo;
            var keyType = GetClrType(mapTypeInfo.KeyTypeCode, mapTypeInfo.KeyTypeInfo);
            var valueType = GetClrType(mapTypeInfo.ValueTypeCode, mapTypeInfo.ValueTypeInfo);
            var openType = typeof(IDictionary<,>);
            return openType.MakeGenericType(keyType, valueType);
        }

        internal Type GetClrTypeForGraph(IColumnInfo typeInfo)
        {
            var mapTypeInfo = (MapColumnInfo)typeInfo;
            var keyType = GetClrTypeForGraph(mapTypeInfo.KeyTypeCode, mapTypeInfo.KeyTypeInfo);
            var valueType = GetClrTypeForGraph(mapTypeInfo.ValueTypeCode, mapTypeInfo.ValueTypeInfo);
            var openType = typeof(IDictionary<,>);
            return openType.MakeGenericType(keyType, valueType);
        }

        public override byte[] Serialize(ushort protocolVersion, IDictionary value)
        {
            var bufferList = new LinkedList<byte[]>();
            var totalLength = 0;
            var totalLengthBuffer = EncodeCollectionLength(protocolVersion, value.Count);
            bufferList.AddLast(totalLengthBuffer);
            totalLength += totalLengthBuffer.Length;
            //We are using IEnumerable, we don't know the length of the underlying collection.
            foreach (DictionaryEntry entry in value)
            {
                if (entry.Value == null)
                {
                    throw new ArgumentNullException("key:" + entry.Key, "Null values are not supported inside collections");
                }
                totalLength += AddItem(bufferList, protocolVersion, entry.Key);
                totalLength += AddItem(bufferList, protocolVersion, entry.Value);
            }
            return Utils.JoinBuffers(bufferList, totalLength);
        }

        public int AddItem(LinkedList<byte[]> bufferList, ushort protocolVersion, object item)
        {
            var keyBuffer = SerializeChild(protocolVersion, item);
            var keyLengthBuffer = EncodeCollectionLength(protocolVersion, keyBuffer.Length);
            bufferList.AddLast(keyLengthBuffer);
            bufferList.AddLast(keyBuffer);
            return keyLengthBuffer.Length + keyBuffer.Length;
        }
    }
}
