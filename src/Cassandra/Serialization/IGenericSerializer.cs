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

namespace Cassandra.Serialization
{
    /// <summary>
    /// Serializer that handles any protocol version.
    /// </summary>
    internal interface IGenericSerializer
    {
        object Deserialize(ProtocolVersion version, byte[] buffer, int offset, int length, ColumnTypeCode typeCode, IColumnInfo typeInfo);

        byte[] Serialize(ProtocolVersion version, object value);

        Type GetClrType(ColumnTypeCode typeCode, IColumnInfo typeInfo);

        Type GetClrTypeForGraph(ColumnTypeCode typeCode, IColumnInfo typeInfo);

        Type GetClrTypeForCustom(IColumnInfo typeInfo);

        ColumnTypeCode GetCqlType(Type type, out IColumnInfo typeInfo);

        /// <summary>
        /// Performs a lightweight validation to determine if the source type and target type matches.
        /// It isn't more strict to support miscellaneous uses of the driver, like direct inputs of blobs and all that. (backward compatibility)
        /// </summary>
        bool IsAssignableFrom(CqlColumn column, object value);

        UdtMap GetUdtMapByName(string name);

        UdtMap GetUdtMapByType(Type type);
    }
}