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
using System.Reflection;

namespace Cassandra.Serialization
{
    /// <summary>
    /// A type serializer that handles list and set CQL types.
    /// </summary>
    internal class CollectionSerializer : TypeSerializer<IEnumerable>
    {
        public override ColumnTypeCode CqlType
        {
            get { throw new NotSupportedException("CollectionSerializer does not represent to a single CQL type"); }
        }

        public override IEnumerable Deserialize(ushort protocolVersion, byte[] buffer, int offset, int length, IColumnInfo typeInfo)
        {
            ColumnTypeCode? childTypeCode = null;
            IColumnInfo childTypeInfo = null;
            var listInfo = typeInfo as ListColumnInfo;
            if (listInfo != null)
            {
                childTypeCode = listInfo.ValueTypeCode;
                childTypeInfo = listInfo.ValueTypeInfo;
            }
            var setInfo = typeInfo as SetColumnInfo;
            if (setInfo != null)
            {
                childTypeCode = setInfo.KeyTypeCode;
                childTypeInfo = setInfo.KeyTypeInfo;
            }
            if (childTypeCode == null)
            {
                throw new DriverInternalError(string.Format("CollectionSerializer can not deserialize CQL values of type {0}",
                    typeInfo == null ? "null" : typeInfo.GetType().FullName));   
            }
            var count = DecodeCollectionLength((ProtocolVersion)protocolVersion, buffer, ref offset);
            var childType = GetClrType(childTypeCode.Value, childTypeInfo);
            var result = Array.CreateInstance(childType, count);
            bool? isNullable = null;
            for (var i = 0; i < count; i++)
            {
                var itemLength = DecodeCollectionLength((ProtocolVersion)protocolVersion, buffer, ref offset);
                if (itemLength < 0)
                {
                    if (isNullable == null)
                    {
                        isNullable = !childType.GetTypeInfo().IsValueType;
                    }

                    if (!isNullable.Value)
                    {
                        var nullableType = typeof(Nullable<>).MakeGenericType(childType);
                        var newResult = Array.CreateInstance(nullableType, count);
                        for (var j = 0; j < i; j++)
                        {
                            newResult.SetValue(result.GetValue(j), j);
                        }
                        result = newResult;
                        childType = nullableType;
                        isNullable = true;
                    }
                    
                    result.SetValue(null, i);
                }
                else
                {
                    result.SetValue(DeserializeChild(protocolVersion, buffer, offset, itemLength, childTypeCode.Value, childTypeInfo), i);
                    offset += itemLength;
                }
            }
            return result;
        }

        internal Type GetClrTypeForList(IColumnInfo typeInfo)
        {
            var valueType = GetClrType(((ListColumnInfo)typeInfo).ValueTypeCode, ((ListColumnInfo)typeInfo).ValueTypeInfo);
            var openType = typeof(IEnumerable<>);
            return openType.MakeGenericType(valueType);
        }
        internal Type GetClrTypeForGraphList(IColumnInfo typeInfo)
        {
            var valueType = GetClrTypeForGraph(((ListColumnInfo)typeInfo).ValueTypeCode, ((ListColumnInfo)typeInfo).ValueTypeInfo);
            var openType = typeof(IEnumerable<>);
            return openType.MakeGenericType(valueType);
        }

        internal Type GetClrTypeForSet(IColumnInfo typeInfo)
        {
            var valueType = GetClrType(((SetColumnInfo)typeInfo).KeyTypeCode, ((SetColumnInfo)typeInfo).KeyTypeInfo);
            var openType = typeof(IEnumerable<>);
            return openType.MakeGenericType(valueType);
        }
        
        internal Type GetClrTypeForGraphSet(IColumnInfo typeInfo)
        {
            var valueType = GetClrTypeForGraph(((SetColumnInfo)typeInfo).KeyTypeCode, ((SetColumnInfo)typeInfo).KeyTypeInfo);
            var openType = typeof(ISet<>);
            return openType.MakeGenericType(valueType);
        }

        public override byte[] Serialize(ushort protocolVersion, IEnumerable value)
        {
            //protocol format: [n items][bytes_1][bytes_n]
            //where the amount of bytes to express the length are 2 or 4 depending on the protocol version
            var bufferList = new LinkedList<byte[]>();
            var totalLength = 0;
            var itemCount = 0;
            //We are using IEnumerable, we don't know the length of the underlying collection.
            foreach (var item in value)
            {
                if (item == null)
                {
                    throw new ArgumentNullException(null, "Null values are not supported inside collections");
                }
                var itemBuffer = SerializeChild(protocolVersion, item);
                var lengthBuffer = EncodeCollectionLength(protocolVersion, itemBuffer.Length);
                bufferList.AddLast(lengthBuffer);
                bufferList.AddLast(itemBuffer);
                totalLength += lengthBuffer.Length + itemBuffer.Length;
                itemCount++;
            }
            var totalLengthBuffer = EncodeCollectionLength(protocolVersion, itemCount);
            bufferList.AddFirst(totalLengthBuffer);
            totalLength += totalLengthBuffer.Length;
            return Utils.JoinBuffers(bufferList, totalLength);
        }
    }
}
