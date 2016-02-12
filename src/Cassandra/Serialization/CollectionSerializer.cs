using System;
using System.Collections;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace Cassandra.Serialization
{
    internal class CollectionSerializer : TypeSerializer<IEnumerable>
    {
        private Serializer _serializer;

        public override ColumnTypeCode CqlType
        {
            get { throw new NotSupportedException("CollectionSerializer does not map to a single cql type"); }
        }

        internal void SetChildSerializer(Serializer serializer)
        {
            _serializer = serializer;
        }

        private byte[] SerializeChild(object obj)
        {
            if (_serializer == null)
            {
                throw new NullReferenceException("Child serializer can not be null");
            }
            return _serializer.Serialize(obj);
        }

        private object DeserializeChild(byte[] buffer, ColumnTypeCode typeCode, IColumnInfo typeInfo)
        {
            if (_serializer == null)
            {
                throw new NullReferenceException("Child serializer can not be null");
            }
            return _serializer.Deserialize(buffer, typeCode, typeInfo);
        }

        public override IEnumerable Deserialize(ushort protocolVersion, byte[] buffer, IColumnInfo typeInfo)
        {
            throw new NotImplementedException();
        }

        public override byte[] Serialize(ushort protocolVersion, IEnumerable value)
        {
            throw new NotImplementedException();
        }

        private IEnumerable DeserializeCollection(int protocolVersion, Type childType, ColumnTypeCode childTypeCode, IColumnInfo childTypeInfo, byte[] buffer)
        {
            var index = 0;
            var count = DecodeCollectionLength(protocolVersion, buffer, ref index);
            var result = Array.CreateInstance(childType, count);
            for (var i = 0; i < count; i++)
            {
                var valueBufferLength = DecodeCollectionLength(protocolVersion, buffer, ref index);
                var itemBuffer = new byte[valueBufferLength];
                Buffer.BlockCopy(buffer, index, itemBuffer, 0, valueBufferLength);
                index += valueBufferLength;
                result.SetValue(DeserializeChild(itemBuffer, childTypeCode, childTypeInfo), i);
            }
            return result;
        }
        
        /// <summary>
        /// Decodes length for collection types depending on the protocol version
        /// </summary>
        private static int DecodeCollectionLength(int protocolVersion, byte[] buffer, ref int index)
        {
            int result;
            if (protocolVersion < 3)
            {
                //length is a short
                result = BeConverter.ToInt16(buffer, index);
                index += 2;
            }
            else
            {
                //length is expressed in int
                result = BeConverter.ToInt32(buffer, index);
                index += 4;
            }
            return result;
        }

    }
}
