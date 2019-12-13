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

namespace Cassandra.IntegrationTests.SimulacronAPI
{
    public class DataType
    {
        public string Value { get; }

        public DataType(string value)
        {
            Value = value;
        }
        
        public static readonly DataType Text = new DataType("text");

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
        
        public static readonly DataType Empty = new DataType("empty");

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
        
        private static readonly Dictionary<Type, DataType> CqlTypeNames = new Dictionary<Type, DataType>
        {
            {typeof (Int32), DataType.Int},
            {typeof (Int64), DataType.BigInt},
            {typeof (String), DataType.Ascii},
            {typeof (byte[]), DataType.Blob},
            {typeof (Boolean), DataType.Boolean},
            {typeof (Decimal), DataType.Decimal},
            {typeof (Double), DataType.Double},
            {typeof (Single), DataType.Float},
            {typeof (Guid), DataType.Uuid},
            {typeof (TimeUuid), DataType.TimeUuid},
            {typeof (DateTimeOffset), DataType.Timestamp},
            {typeof (DateTime), DataType.Timestamp},
        };

        public static DataType GetDataType(object obj)
        {
            if (obj == null)
            {
                return DataType.Ascii;
            }

            var type = obj.GetType();
            return DataType.GetDataType(type);
        }

        public static DataType GetDataType(Type type)
        {
            if (type == null)
            {
                return DataType.Ascii;
            }

            if (type.Name.Equals("Nullable`1"))
            {
                return DataType.GetDataType(type.GetGenericArguments()[0]);
            }

            if (DataType.CqlTypeNames.ContainsKey(type))
            {
                return DataType.CqlTypeNames[type];
            }

            if (type.IsGenericType)
            {
                if (type.Name.Equals("Nullable`1"))
                {
                    return DataType.GetDataType(type.GetGenericArguments()[0]);
                }

                if (type.GetInterface("ISet`1") != null)
                {
                    return DataType.Set(DataType.GetDataType(type.GetGenericArguments()[0]));
                }

                if (type.GetInterface("IDictionary`2") != null)
                {
                    return DataType.Map(
                        DataType.GetDataType(type.GetGenericArguments()[0]),
                        DataType.GetDataType(type.GetGenericArguments()[1]));
                }

                if (type.GetInterface("IEnumerable`1") != null)
                {
                    return DataType.List(DataType.GetDataType(type.GetGenericArguments()[0]));
                }
            }
            else if (type.Name == "BigDecimal")
            {
                return DataType.Decimal;
            }

            throw new ArgumentException("no type found for dotnet type " + type.Name);
        }
        
        internal static readonly DateTimeOffset UnixStart = new DateTimeOffset(1970, 1, 1, 0, 0, 0, 0, TimeSpan.Zero);

        public static long GetTimestamp(DateTimeOffset dt)
        {
            return DataType.GetMicroSecondsTimestamp(dt) / 1000;
        }
        
        public static long GetMicroSecondsTimestamp(DateTimeOffset dt)
        {
            var ticks = (dt - UnixStart).Ticks;
            return ticks / (TimeSpan.TicksPerMillisecond / 1000);
        }

        // missing types:
        //public static final int CUSTOM = 0x0000;
        //public static final int UDT = 0x0030;
        //public static final int TUPLE = 0x0031;
    }
}