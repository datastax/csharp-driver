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
using System.Globalization;
using System.Linq;
using System.Numerics;
using Cassandra.Tests.Extensions.Serializers;

namespace Cassandra.IntegrationTests.SimulacronAPI
{
    public class DataType
    {
        private const string ListTypeName = "org.apache.cassandra.db.marshal.ListType";
        private const string SetTypeName = "org.apache.cassandra.db.marshal.SetType";
        private const string MapTypeName = "org.apache.cassandra.db.marshal.MapType";
        private const string UdtTypeName = "org.apache.cassandra.db.marshal.UserType";
        private const string TupleTypeName = "org.apache.cassandra.db.marshal.TupleType";
        private const string FrozenTypeName = "org.apache.cassandra.db.marshal.FrozenType";
        private const string ReversedTypeName = "org.apache.cassandra.db.marshal.ReversedType";
        private const string CompositeTypeName = "org.apache.cassandra.db.marshal.CompositeType";
        private const string EmptyTypeName = "org.apache.cassandra.db.marshal.EmptyType";

        static DataType()
        {
            DataType.SingleFqTypeNames = new Dictionary<DataType, string>
            {
                {DataType.Text, "org.apache.cassandra.db.marshal.UTF8Type"},
                {DataType.Ascii, "org.apache.cassandra.db.marshal.AsciiType"},
                {DataType.Varchar, "org.apache.cassandra.db.marshal.UTF8Type"},
                {DataType.Uuid, "org.apache.cassandra.db.marshal.UUIDType"},
                {DataType.TimeUuid, "org.apache.cassandra.db.marshal.TimeUUIDType"},
                {DataType.Int, "org.apache.cassandra.db.marshal.Int32Type"},
                {DataType.Blob, "org.apache.cassandra.db.marshal.BytesType"},
                {DataType.Float, "org.apache.cassandra.db.marshal.FloatType"},
                {DataType.Double, "org.apache.cassandra.db.marshal.DoubleType"},
                {DataType.Boolean, "org.apache.cassandra.db.marshal.BooleanType"},
                {DataType.Inet, "org.apache.cassandra.db.marshal.InetAddressType"},
                {DataType.Date, "org.apache.cassandra.db.marshal.SimpleDateType"},
                {DataType.Time, "org.apache.cassandra.db.marshal.TimeType"},
                {DataType.SmallInt, "org.apache.cassandra.db.marshal.ShortType"},
                {DataType.TinyInt, "org.apache.cassandra.db.marshal.ByteType"},
                {DataType.Timestamp, "org.apache.cassandra.db.marshal.TimestampType"},
                {DataType.BigInt, "org.apache.cassandra.db.marshal.LongType"},
                {DataType.Decimal, "org.apache.cassandra.db.marshal.DecimalType"},
                {DataType.VarInt, "org.apache.cassandra.db.marshal.IntegerType"},
                {DataType.Counter, "org.apache.cassandra.db.marshal.CounterColumnType"}
            };
        }

        public string GetFqTypeName()
        {
            if (InnerTypes == null)
            {
                return DataType.SingleFqTypeNames[this];
            }

            if (Value.StartsWith("set<"))
            {
                return $"{DataType.SetTypeName}({InnerTypes.Single().GetFqTypeName()})";
            }
            else if (Value.StartsWith("map<"))
            {
                return $"{DataType.MapTypeName}({InnerTypes.First().GetFqTypeName()},{InnerTypes.Skip(1).First().GetFqTypeName()})";
            }
            else if (Value.StartsWith("list<"))
            {
                return $"{DataType.ListTypeName}({InnerTypes.Single().GetFqTypeName()})";
            }
            else if (Value.StartsWith("frozen<"))
            {
                return $"{DataType.FrozenTypeName}({InnerTypes.Single().GetFqTypeName()})";
            }
            else if (Value.StartsWith("tuple<"))
            {
                return $"{DataType.TupleTypeName}({string.Join(",", InnerTypes.Select(i => i.GetFqTypeName()))})";
            }

            throw new InvalidOperationException("Unrecognized data type.");
        }

        private static readonly Dictionary<DataType, string> SingleFqTypeNames;

        private IEnumerable<DataType> InnerTypes { get; }

        public string Value { get; }

        public DataType(string value)
        {
            Value = value;
        }
        
        private DataType(string value, params DataType[] innerTypes)
        {
            Value = value;
            InnerTypes = innerTypes;
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
        
        public static DataType Udt(string name)
        {
            return new DataType(name);
        }

        public static DataType Custom(string type)
        {
            return new DataType($"'{type}'");
        }

        public static DataType Frozen(DataType dataType)
        {
            return new DataType($"frozen<{dataType.Value}>", dataType);
        }

        public static DataType List(DataType dataType)
        {
            return new DataType($"list<{dataType.Value}>", dataType);
        }

        public static DataType Set(DataType dataType)
        {
            return new DataType($"set<{dataType.Value}>", dataType);
        }

        public static DataType Map(DataType dataTypeKey, DataType dataTypeValue)
        {
            return new DataType($"map<{dataTypeKey.Value},{dataTypeValue.Value}>", dataTypeKey, dataTypeValue);
        }
        
        public static DataType Tuple(params DataType[] dataTypes)
        {
            return new DataType($"tuple<{string.Join(",", dataTypes.Select(d => d.Value))}>", dataTypes);
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
            {typeof (BigDecimal), DataType.Decimal},
            {typeof (BigInteger), DataType.VarInt},
        };

        public static DataType GetDataType(object obj)
        {
            if (obj == null)
            {
                throw new InvalidOperationException("object can't be null in order for type inference to work");
            }

            var type = obj.GetType();
            return DataType.GetDataType(type);
        }

        public static DataType GetDataType(Type type)
        {
            if (type == null)
            {
                throw new InvalidOperationException("object can't be null in order for type inference to work");
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
                
                if (type.Name.StartsWith("Tuple"))
                {
                    return DataType.Tuple(type.GetGenericArguments().Select(DataType.GetDataType).ToArray());
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

            throw new ArgumentException("no type found for dotnet type " + type.Name);
        }
        
        internal static readonly DateTimeOffset UnixStart = new DateTimeOffset(1970, 1, 1, 0, 0, 0, 0, TimeSpan.Zero);

        public static string ByteArrayToString(byte[] ba)
        {
            return "0x" + ba.Aggregate(string.Empty, (acc, b) => $"{acc}{b:x2}");
        }

        public static object AdaptForSimulacronPrime(object value)
        {
            if (value is DateTimeOffset dateTimeOffset)
            {
                return DataType.GetTimestamp(dateTimeOffset);
            }

            if (value is DateTime dt)
            {
                return DataType.GetTimestamp(new DateTimeOffset(dt));
            }

            if (value is TimeUuid)
            {
                return value.ToString();
            }

            if (value is byte[] v)
            {
                return DataType.ByteArrayToString(v);
            }

            if (value is decimal v1)
            {
                return v1.ToString(CultureInfo.InvariantCulture);
            }

            if (value is BigDecimal v2)
            {
                return v2.ToString();
            }

            return value;
        }

        public static long GetTimestamp(DateTimeOffset dt)
        {
            return DataType.GetMicroSecondsTimestamp(dt) / 1000;
        }
        
        public static long GetMicroSecondsTimestamp(DateTimeOffset dt)
        {
            var ticks = (dt - UnixStart).Ticks;
            return ticks / (TimeSpan.TicksPerMillisecond / 1000);
        }

        public override bool Equals(object obj)
        {
            return obj is DataType type &&
                   (InnerTypes == null
                       ? type.InnerTypes == null
                       : (type.InnerTypes != null && InnerTypes.SequenceEqual(type.InnerTypes))) &&
                   Value == type.Value;
        }

        public override int GetHashCode()
        {
            var hashCode = -1251219967;

            if (InnerTypes != null)
            {
                foreach (var t in InnerTypes)
                {
                    hashCode = hashCode * -1521134295 + t.GetHashCode();
                }
            }

            hashCode = hashCode * -1521134295 + EqualityComparer<string>.Default.GetHashCode(Value);
            return hashCode;
        }

        // missing types:
        //public static final int CUSTOM = 0x0000;
        //public static final int UDT = 0x0030;
        //public static final int TUPLE = 0x0031;
    }
}