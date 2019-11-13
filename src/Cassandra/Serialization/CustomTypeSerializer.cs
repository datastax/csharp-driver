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

namespace Cassandra.Serialization
{
    /// <summary>
    /// Base serializer for custom types.
    /// </summary>
    public abstract class CustomTypeSerializer<T> : TypeSerializer<T>
    {
        private readonly IColumnInfo _typeInfo;

        public override ColumnTypeCode CqlType
        {
            get { return ColumnTypeCode.Custom; }
        }

        public override IColumnInfo TypeInfo
        {
            get { return _typeInfo; }
        }

        /// <summary>
        /// Creates a new instance of the serializer for custom types.
        /// </summary>
        /// <param name="name">Fully qualified name of the custom type</param>
        protected CustomTypeSerializer(string name)
        {
            _typeInfo = new CustomColumnInfo(name);
        }
    }
}
