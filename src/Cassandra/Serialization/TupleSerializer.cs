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
using System.Linq;
using System.Reflection;

namespace Cassandra.Serialization
{
    internal class TupleSerializer : TypeSerializer<IStructuralEquatable>
    {
        public override ColumnTypeCode CqlType
        {
            get { return ColumnTypeCode.Tuple; }
        }

        public override IStructuralEquatable Deserialize(ushort protocolVersion, byte[] buffer, int offset, int length, IColumnInfo typeInfo)
        {
            var tupleInfo = (TupleColumnInfo)typeInfo;
            var tupleType = GetClrType(ColumnTypeCode.Tuple, tupleInfo);
            var tupleValues = new object[tupleInfo.Elements.Count];
            var maxOffset = offset + length;
            for (var i = 0; i < tupleInfo.Elements.Count; i++)
            {
                var element = tupleInfo.Elements[i];
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
                tupleValues[i] = DeserializeChild(protocolVersion, buffer, offset, itemLength, element.TypeCode, element.TypeInfo);
                offset += itemLength;
            }

            return (IStructuralEquatable)Activator.CreateInstance(tupleType, tupleValues);
        }

        internal Type GetClrType(IColumnInfo typeInfo)
        {
            var tupleInfo = (TupleColumnInfo)typeInfo;
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

        public override byte[] Serialize(ushort protocolVersion, IStructuralEquatable value)
        {
            var tupleType = value.GetType();
            var subtypes = tupleType.GetTypeInfo().GetGenericArguments();
            var bufferList = new List<byte[]>();
            var bufferLength = 0;
            for (var i = 1; i <= subtypes.Length; i++)
            {
                var prop = tupleType.GetTypeInfo().GetProperty("Item" + i);
                if (prop != null)
                {
                    var buffer = SerializeChild(protocolVersion, prop.GetValue(value, null));
                    bufferList.Add(buffer);
                    if (buffer != null)
                    {
                        bufferLength += buffer.Length;
                    }
                }
            }
            return EncodeBufferList(bufferList, bufferLength);
        }
    }
}
