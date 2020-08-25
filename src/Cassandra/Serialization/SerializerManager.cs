//
//       Copyright (C) DataStax Inc.
//
//    Licensed under the Apache License, Version 2.0 (the "License");
//    you may not use this file except in compliance with the License.
//    You may obtain a copy of the License at
//
//       http://www.apache.org/licenses/LICENSE-2.0
//
//    Unless required by applicable law or agreed to in writing, software
//    distributed under the License is distributed on an "AS IS" BASIS,
//    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//    See the License for the specific language governing permissions and
//    limitations under the License.

using System.Collections.Generic;

namespace Cassandra.Serialization
{
    /// <summary>
    /// Handles types serialization from binary form to objects and the other way around.
    /// The instance is aware of protocol version, custom codecs, UDT mappers
    /// </summary>
    internal class SerializerManager : ISerializerManager
    {
        internal static readonly ISerializerManager Default = new SerializerManager(ProtocolVersion.V1);

        /// <summary>
        /// An instance of a buffer that represents the value Unset
        /// </summary>
        internal static readonly byte[] UnsetBuffer = new byte[0];

        private readonly GenericSerializer _genericSerializer;
        private volatile ISerializer _serializer;

        internal SerializerManager(ProtocolVersion protocolVersion, IEnumerable<ITypeSerializer> typeSerializers = null)
        {
            _genericSerializer = new GenericSerializer(typeSerializers);
            _serializer = new Serializer(protocolVersion, _genericSerializer);
        }

        public ProtocolVersion CurrentProtocolVersion => _serializer.ProtocolVersion;

        public void ChangeProtocolVersion(ProtocolVersion version)
        {
            _serializer = new Serializer(version, _genericSerializer);
        }

        public ISerializer GetCurrentSerializer()
        {
            return _serializer;
        }

        public object Deserialize(ProtocolVersion version, byte[] buffer, int offset, int length, ColumnTypeCode typeCode, IColumnInfo typeInfo)
        {
            return _genericSerializer.Deserialize(version, buffer, offset, length, typeCode, typeInfo);
        }

        public byte[] Serialize(ProtocolVersion version, object value)
        {
            return _genericSerializer.Serialize(version, value);
        }
        
        public void SetUdtMap(string name, UdtMap map)
        {
            _genericSerializer.SetUdtMap(name, map);
        }

        public IGenericSerializer GetGenericSerializer()
        {
            return _genericSerializer;
        }
    }
}