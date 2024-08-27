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
using System.Threading;

namespace Cassandra.Serialization
{
    internal class VectorSerializer : TypeSerializer<IInternalCqlVector>
    {
        private static readonly ThreadLocal<byte[]> EncodingBuffer = new ThreadLocal<byte[]>(() => new byte[9]);

        public override ColumnTypeCode CqlType => ColumnTypeCode.Custom;

        public override IColumnInfo TypeInfo => new CustomColumnInfo(DataTypeParser.VectorTypeName);

        public override IInternalCqlVector Deserialize(ushort protocolVersion, byte[] buffer, int offset, int length, IColumnInfo typeInfo)
        {
            var vectorTypeInfo = GetVectorColumnInfo(typeInfo);
            if (vectorTypeInfo.Dimension == null)
            {
                throw new DriverInternalError("The driver needs to know the vector dimension when deserializing vectors.");
            }

            var childSerializer = GetChildSerializer();
            var childType = GetClrType(vectorTypeInfo.ValueTypeCode, vectorTypeInfo.ValueTypeInfo);
            var result = Array.CreateInstance(childType, vectorTypeInfo.Dimension.Value);
            for (var i = 0; i < vectorTypeInfo.Dimension; i++)
            {
                var itemLength = childSerializer.GetValueLengthIfFixed(vectorTypeInfo.ValueTypeCode, vectorTypeInfo.ValueTypeInfo);
                if (itemLength < 0)
                {
                    var longItemLength = VintSerializer.ReadUnsignedVInt(buffer, ref offset);
                    if (longItemLength > int.MaxValue)
                    {
                        throw new DriverInternalError(
                            "The driver doesn't support item lengths greater than int.MaxSize during deserialization and serialization.");
                    }

                    itemLength = Convert.ToInt32(longItemLength);
                }
                result.SetValue(DeserializeChild(protocolVersion, buffer, offset, itemLength, vectorTypeInfo.ValueTypeCode, vectorTypeInfo.ValueTypeInfo), i);
                offset += itemLength;
            }
            var vectorSubType = typeof(CqlVector<>).MakeGenericType(childType);
            var vector = (IInternalCqlVector)Activator.CreateInstance(vectorSubType, nonPublic: true);
            vector.SetArray(result);
            return vector;
        }

        public override byte[] Serialize(ushort protocolVersion, IInternalCqlVector value)
        {
            var childSerializer = GetChildSerializer();
            _ = childSerializer.GetCqlType(value.GetType(), out var columnInfo);
            var vectorTypeInfo = GetVectorColumnInfo(columnInfo);
            var itemLength = childSerializer.GetValueLengthIfFixed(vectorTypeInfo.ValueTypeCode, vectorTypeInfo.ValueTypeInfo);
            var bufferList = new LinkedList<byte[]>();
            var totalLength = 0;
            foreach (var item in value)
            {
                if (item == null)
                {
                    throw new ArgumentNullException(null, "Null values are not supported inside vectors");
                }

                var itemBuffer = SerializeChild(protocolVersion, item);
                if (itemLength < 0)
                {
                    var vIntSize = VintSerializer.WriteUnsignedVInt(itemBuffer.Length, EncodingBuffer.Value);
                    var lengthBuffer = new byte[vIntSize];
                    Buffer.BlockCopy(EncodingBuffer.Value, 0, lengthBuffer, 0, vIntSize);
                    bufferList.AddLast(lengthBuffer);
                    totalLength += lengthBuffer.Length;
                }
                bufferList.AddLast(itemBuffer);
                totalLength += itemBuffer.Length;
            }
            return Utils.JoinBuffers(bufferList, totalLength);
        }

        internal Type GetClrType(VectorColumnInfo vectorColumnInfo)
        {
            var valueType = GetClrType(vectorColumnInfo.ValueTypeCode, vectorColumnInfo.ValueTypeInfo);
            var openType = typeof(CqlVector<>);
            return openType.MakeGenericType(valueType);
        }

        private VectorColumnInfo GetVectorColumnInfo(IColumnInfo typeInfo)
        {
            if (typeInfo is VectorColumnInfo vectorColumnInfo)
            {
                return vectorColumnInfo;
            }

            throw new DriverInternalError(
                $"VectorSerializer can not deserialize CQL values of type {(typeInfo == null ? "null" : typeInfo.GetType().FullName)}");
        }
    }
}
