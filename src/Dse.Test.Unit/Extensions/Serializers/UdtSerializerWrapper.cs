//
//  Copyright (C) 2017 DataStax, Inc.
//
//  Please see the license for details:
//  http://www.datastax.com/terms/datastax-dse-driver-license-terms
//

using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Cassandra.Serialization;

namespace Cassandra.Tests.Extensions.Serializers
{
    public class UdtSerializerWrapper : UdtSerializer
    {
        private readonly bool _fixedValue;
        public int SerializationCounter { get; private set; }

        public int DeserializationCounter { get; private set; }

        public UdtSerializerWrapper(bool fixedValue = true)
        {
            _fixedValue = fixedValue;
        }

        public override byte[] Serialize(ushort protocolVersion, object value)
        {
            SerializationCounter++;
            if (_fixedValue)
            {
                return Encoding.UTF8.GetBytes("DUMMY UDT SERIALIZED");   
            }
            return base.Serialize(protocolVersion, value);
        }

        public override object Deserialize(ushort protocolVersion, byte[] buffer, int offset, int length, IColumnInfo typeInfo)
        {
            DeserializationCounter++;
            if (_fixedValue)
            {
                return Utils.SliceBuffer(buffer, offset, length);   
            }
            return base.Deserialize(protocolVersion, buffer, offset, length, typeInfo);
        }
    }
}
