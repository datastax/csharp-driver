using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace Cassandra.Serialization
{
    public class UdtSerializer : TypeSerializer<object>
    {
        private Serializer _serializer;

        public override ColumnTypeCode CqlType
        {
            get { return ColumnTypeCode.Udt; }
        }

        protected internal UdtSerializer()
        {

        }

        internal void SetChildSerializer(Serializer serializer)
        {
            _serializer = serializer;
        }

        protected internal UdtMap GetUdtMap(UdtColumnInfo typeInfo)
        {
            throw new NotImplementedException();
        }

        protected UdtMap GetUdtMap(Type type)
        {
            throw new NotImplementedException();
        }

        protected byte[] SerializeChild(object obj)
        {
            if (_serializer == null)
            {
                throw new NullReferenceException("Child serializer can not be null");
            }
            return _serializer.Serialize(obj);
        }

        protected object DeserializeChild(byte[] buffer, ColumnTypeCode typeCode, IColumnInfo typeInfo)
        {
            if (_serializer == null)
            {
                throw new NullReferenceException("Child serializer can not be null");
            }
            return _serializer.Deserialize(buffer, typeCode, typeInfo);
        }

        public override object Deserialize(ushort protocolVersion, byte[] buffer, IColumnInfo typeInfo)
        {
            var udtInfo = (UdtColumnInfo)typeInfo;
            var map = GetUdtMap(udtInfo);
            if (map == null)
            {
                return buffer;
            }
            var valuesList = new object[udtInfo.Fields.Count];
            var offset = 0;
            for (var i = 0; i < udtInfo.Fields.Count; i++)
            {
                var field = udtInfo.Fields[i];
                if (offset >= buffer.Length)
                {
                    break;
                }
                var length = BeConverter.ToInt32(buffer, offset);
                offset += 4;
                if (length < 0)
                {
                    continue;
                }
                var itemBuffer = Utils.SliceBuffer(buffer, offset, length);
                offset += length;
                valuesList[i] = DeserializeChild(itemBuffer, field.TypeCode, field.TypeInfo);
            }
            return map.ToObject(valuesList);
        }

        public override byte[] Serialize(ushort protocolVersion, object value)
        {
            if (value == null)
            {
                throw new ArgumentNullException("value");
            }
            throw new NotImplementedException();
//            var map = GetUdtMap(obj.GetType());
//            var bufferList = new List<byte[]>();
//            var bufferLength = 0;
//            foreach (var field in map.Definition.Fields)
//            {
//                object fieldValue = null;
//                var prop = map.GetPropertyForUdtField(field.Name);
//                if (prop != null)
//                {
//                    fieldValue = prop.GetValue(obj, null);
//                }
//                var itemBuffer = Encode(protocolVersion, fieldValue);
//                bufferList.Add(itemBuffer);
//                if (fieldValue != null)
//                {
//                    bufferLength += itemBuffer.Length;
//                }
//            }
//            return EncodeBufferList(bufferList, bufferLength);
        }
    }
}
