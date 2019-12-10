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

namespace Cassandra.IntegrationTests.SimulacronAPI
{
    public class DataType
    {
        public string Value { get; }

        public DataType(string value)
        {
            Value = value;
        }

        public static readonly DataType Ascii = new DataType("ascii");

        public static readonly DataType BigInt = new DataType("bigint");

        public static readonly DataType Blob = new DataType("blob");

        public static readonly DataType Boolean = new DataType("boolean");

        public static readonly DataType Counter = new DataType("counter");

        public static readonly DataType Decimal = new DataType("decimal");

        public static readonly DataType Double = new DataType("double");

        public static readonly DataType Float = new DataType("float");

        public static readonly DataType Int = new DataType("int");

        public static readonly DataType Timestamp = new DataType("timestamp");

        public static readonly DataType Uuid = new DataType("uuid");

        public static readonly DataType Varchar = new DataType("varchar");

        public static readonly DataType VarInt = new DataType("varint");

        public static readonly DataType TimeUuid = new DataType("timeuuid");

        public static readonly DataType Inet = new DataType("inet");

        public static readonly DataType Date = new DataType("date");

        public static readonly DataType Time = new DataType("time");

        public static readonly DataType SmallInt = new DataType("smallint");

        public static readonly DataType TinyInt = new DataType("tinyint");

        public static readonly DataType Duration = new DataType("duration"); // v5+

        public static DataType List(DataType dataType)
        {
            return new DataType($"list<{dataType.Value}>");
        }

        public static DataType Set(DataType dataType)
        {
            return new DataType($"set<{dataType.Value}>");
        }

        public static DataType Map(DataType dataTypeKey, DataType dataTypeValue)
        {
            return new DataType($"map<{dataTypeKey.Value},{dataTypeValue.Value}>");
        }

        // missing types:
        //public static final int CUSTOM = 0x0000;
        //public static final int UDT = 0x0030;
        //public static final int TUPLE = 0x0031;
    }
}