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

namespace Cassandra
{
    /// <summary>
    /// Exception that is thrown when the driver expects a 'blob' type from the server for an encrypted column but received something else.
    /// </summary>
    public class ColumnEncryptionInvalidTypeError : DriverException
    {
        public string ColumnName { get; }

        public ColumnTypeCode ColumnSchemaTypeCode { get; }

        public object DeserializedValue { get; }

        public ColumnEncryptionInvalidTypeError(string column, ColumnTypeCode typeCode, object deserializedValue)
            : base($"Deserialized value's type for encrypted column {column} is {deserializedValue.GetType().AssemblyQualifiedName} but should be 'byte[]'. " +
                   $"Ensure the type of encrypted columns is 'blob' on the schema (currently cql type of this column is {typeCode.ToString()})." +
                   "To access the raw deserialized value you can use the DeserializedValue property of this exception. No decryption was performed on this value.")
        {
            ColumnName = column;
            ColumnSchemaTypeCode = typeCode;
            DeserializedValue = deserializedValue;
        }
    }
}
