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
using System.Collections.Generic;
using System.Reflection;

namespace Cassandra.Serialization
{
    internal class VectorSerializer : TypeSerializer<CqlVector>
    {
        public override ColumnTypeCode CqlType => ColumnTypeCode.Custom;

        public override IColumnInfo TypeInfo => new CustomColumnInfo(DataTypeParser.VectorTypeName);

        public override CqlVector Deserialize(ushort protocolVersion, byte[] buffer, int offset, int length, IColumnInfo typeInfo)
        {
            ColumnTypeCode? childTypeCode = null;
            IColumnInfo childTypeInfo = null;
            int dimension = -1;
            if (typeInfo is VectorColumnInfo vectorColumnInfo)
            {
                childTypeCode = vectorColumnInfo.ValueTypeCode;
                childTypeInfo = vectorColumnInfo.ValueTypeInfo;
                dimension = vectorColumnInfo.Dimension;
            }

            if (childTypeCode == null)
            {
                throw new DriverInternalError(
                    $"VectorSerializer can not deserialize CQL values of type {(typeInfo == null ? "null" : typeInfo.GetType().FullName)}");
            }
            var childType = GetClrType(childTypeCode.Value, childTypeInfo);
            var result = Array.CreateInstance(childType, dimension);
            bool? isNullable = null;
            for (var i = 0; i < dimension; i++)
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
                        var newResult = Array.CreateInstance(nullableType, dimension);
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
            return new CqlVector(result);
        }

        public override byte[] Serialize(ushort protocolVersion, CqlVector value)
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
            return Utils.JoinBuffers(bufferList, totalLength);
        }
    }
}
