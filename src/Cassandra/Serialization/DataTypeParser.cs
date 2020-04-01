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
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Cassandra.Tasks;

namespace Cassandra.Serialization
{
    /// <summary>
    /// A class containing methods for Cql types name parsing.
    /// </summary>
    internal static class DataTypeParser
    {
        private const string ListTypeName = "org.apache.cassandra.db.marshal.ListType";
        private const string SetTypeName = "org.apache.cassandra.db.marshal.SetType";
        private const string MapTypeName = "org.apache.cassandra.db.marshal.MapType";
        public const string UdtTypeName = "org.apache.cassandra.db.marshal.UserType";
        private const string TupleTypeName = "org.apache.cassandra.db.marshal.TupleType";
        private const string FrozenTypeName = "org.apache.cassandra.db.marshal.FrozenType";
        public const string ReversedTypeName = "org.apache.cassandra.db.marshal.ReversedType";
        public const string CompositeTypeName = "org.apache.cassandra.db.marshal.CompositeType";
        private const string EmptyTypeName = "org.apache.cassandra.db.marshal.EmptyType";

        /// <summary>
        /// Contains the cql literals of certain types
        /// </summary>
        private static class CqlNames
        {
            public const string Frozen = "frozen";
            public const string List = "list";
            public const string Set = "set";
            public const string Map = "map";
            public const string Tuple = "tuple";
            public const string Empty = "empty";
        }

        private static readonly Dictionary<string, ColumnTypeCode> SingleFqTypeNames = new Dictionary<string, ColumnTypeCode>()
        {
            {"org.apache.cassandra.db.marshal.UTF8Type", ColumnTypeCode.Varchar},
            {"org.apache.cassandra.db.marshal.AsciiType", ColumnTypeCode.Ascii},
            {"org.apache.cassandra.db.marshal.UUIDType", ColumnTypeCode.Uuid},
            {"org.apache.cassandra.db.marshal.TimeUUIDType", ColumnTypeCode.Timeuuid},
            {"org.apache.cassandra.db.marshal.Int32Type", ColumnTypeCode.Int},
            {"org.apache.cassandra.db.marshal.BytesType", ColumnTypeCode.Blob},
            {"org.apache.cassandra.db.marshal.FloatType", ColumnTypeCode.Float},
            {"org.apache.cassandra.db.marshal.DoubleType", ColumnTypeCode.Double},
            {"org.apache.cassandra.db.marshal.BooleanType", ColumnTypeCode.Boolean},
            {"org.apache.cassandra.db.marshal.InetAddressType", ColumnTypeCode.Inet},
            {"org.apache.cassandra.db.marshal.SimpleDateType", ColumnTypeCode.Date},
            {"org.apache.cassandra.db.marshal.TimeType", ColumnTypeCode.Time},
            {"org.apache.cassandra.db.marshal.ShortType", ColumnTypeCode.SmallInt},
            {"org.apache.cassandra.db.marshal.ByteType", ColumnTypeCode.TinyInt},
            {"org.apache.cassandra.db.marshal.DateType", ColumnTypeCode.Timestamp},
            {"org.apache.cassandra.db.marshal.TimestampType", ColumnTypeCode.Timestamp},
            {"org.apache.cassandra.db.marshal.LongType", ColumnTypeCode.Bigint},
            {"org.apache.cassandra.db.marshal.DecimalType", ColumnTypeCode.Decimal},
            {"org.apache.cassandra.db.marshal.IntegerType", ColumnTypeCode.Varint},
            {"org.apache.cassandra.db.marshal.CounterColumnType", ColumnTypeCode.Counter}
        };

        private static readonly Dictionary<string, ColumnTypeCode> SingleCqlNames = new Dictionary<string, ColumnTypeCode>()
        {
            {"varchar", ColumnTypeCode.Varchar},
            {"text", ColumnTypeCode.Text},
            {"ascii", ColumnTypeCode.Ascii},
            {"uuid", ColumnTypeCode.Uuid},
            {"timeuuid", ColumnTypeCode.Timeuuid},
            {"int", ColumnTypeCode.Int},
            {"blob", ColumnTypeCode.Blob},
            {"float", ColumnTypeCode.Float},
            {"double", ColumnTypeCode.Double},
            {"boolean", ColumnTypeCode.Boolean},
            {"inet", ColumnTypeCode.Inet},
            {"date", ColumnTypeCode.Date},
            {"time", ColumnTypeCode.Time},
            {"smallint", ColumnTypeCode.SmallInt},
            {"tinyint", ColumnTypeCode.TinyInt},
            {"duration", ColumnTypeCode.Duration},
            {"timestamp", ColumnTypeCode.Timestamp},
            {"bigint", ColumnTypeCode.Bigint},
            {"decimal", ColumnTypeCode.Decimal},
            {"varint", ColumnTypeCode.Varint},
            {"counter", ColumnTypeCode.Counter}
        };

        private static readonly int SingleFqTypeNamesLength = SingleFqTypeNames.Keys.OrderByDescending(k => k.Length).First().Length;


        /// <summary>
        /// Parses a given fully-qualified class type name to get the data type information
        /// </summary>
        /// <exception cref="ArgumentException" />
        internal static ColumnDesc ParseFqTypeName(string typeName, int startIndex = 0, int length = 0)
        {
            const StringComparison comparison = StringComparison.Ordinal;
            var dataType = new ColumnDesc
            {
                TypeCode = ColumnTypeCode.Custom
            };
            if (length == 0)
            {
                length = typeName.Length;
            }
            if (length > ReversedTypeName.Length && typeName.IndexOf(ReversedTypeName, startIndex, comparison) == startIndex)
            {
                //move the start index and subtract the length plus parenthesis
                startIndex += ReversedTypeName.Length + 1;
                length -= ReversedTypeName.Length + 2;
                dataType.IsReversed = true;
            }
            if (length > FrozenTypeName.Length && typeName.IndexOf(FrozenTypeName, startIndex, comparison) == startIndex)
            {
                //Remove the frozen
                startIndex += FrozenTypeName.Length + 1;
                length -= FrozenTypeName.Length + 2;
                dataType.IsFrozen = true;
            }
            if (typeName == EmptyTypeName)
            {
                // Set it as custom without type info
                return dataType;
            }
            //Quick check if its a single type
            if (length <= SingleFqTypeNamesLength)
            {
                if (startIndex > 0)
                {
                    typeName = typeName.Substring(startIndex, length);
                }
                if (SingleFqTypeNames.TryGetValue(typeName, out ColumnTypeCode typeCode))
                {
                    dataType.TypeCode = typeCode;
                    return dataType;
                }
            }
            if (typeName.IndexOf(ListTypeName, startIndex, comparison) == startIndex)
            {
                //Its a list
                //org.apache.cassandra.db.marshal.ListType(innerType)
                //move cursor across the name and bypass the parenthesis
                startIndex += ListTypeName.Length + 1;
                length -= ListTypeName.Length + 2;
                var innerTypes = ParseParams(typeName, startIndex, length);
                if (innerTypes.Count != 1)
                {
                    throw GetTypeException(typeName);
                }
                dataType.TypeCode = ColumnTypeCode.List;
                var subType = ParseFqTypeName(innerTypes[0]);
                dataType.TypeInfo = new ListColumnInfo()
                {
                    ValueTypeCode = subType.TypeCode,
                    ValueTypeInfo = subType.TypeInfo
                };
                return dataType;
            }
            if (typeName.IndexOf(SetTypeName, startIndex, comparison) == startIndex)
            {
                //Its a set
                //org.apache.cassandra.db.marshal.SetType(innerType)
                //move cursor across the name and bypass the parenthesis
                startIndex += SetTypeName.Length + 1;
                length -= SetTypeName.Length + 2;
                var innerTypes = ParseParams(typeName, startIndex, length);
                if (innerTypes.Count != 1)
                {
                    throw GetTypeException(typeName);
                }
                dataType.TypeCode = ColumnTypeCode.Set;
                var subType = ParseFqTypeName(innerTypes[0]);
                dataType.TypeInfo = new SetColumnInfo()
                {
                    KeyTypeCode = subType.TypeCode,
                    KeyTypeInfo = subType.TypeInfo
                };
                return dataType;
            }
            if (typeName.IndexOf(MapTypeName, startIndex, comparison) == startIndex)
            {
                //org.apache.cassandra.db.marshal.MapType(keyType,valueType)
                //move cursor across the name and bypass the parenthesis
                startIndex += MapTypeName.Length + 1;
                length -= MapTypeName.Length + 2;
                var innerTypes = ParseParams(typeName, startIndex, length);
                //It should contain the key and value types
                if (innerTypes.Count != 2)
                {
                    throw GetTypeException(typeName);
                }
                dataType.TypeCode = ColumnTypeCode.Map;
                var keyType = ParseFqTypeName(innerTypes[0]);
                var valueType = ParseFqTypeName(innerTypes[1]);
                dataType.TypeInfo = new MapColumnInfo()
                {
                    KeyTypeCode = keyType.TypeCode,
                    KeyTypeInfo = keyType.TypeInfo,
                    ValueTypeCode = valueType.TypeCode,
                    ValueTypeInfo = valueType.TypeInfo
                };
                return dataType;
            }
            if (typeName.IndexOf(UdtTypeName, startIndex, comparison) == startIndex)
            {
                //move cursor across the name and bypass the parenthesis
                startIndex += UdtTypeName.Length + 1;
                length -= UdtTypeName.Length + 2;
                var udtParams = ParseParams(typeName, startIndex, length);
                if (udtParams.Count < 2)
                {
                    //It should contain at least the keyspace, name of the udt and a type
                    throw GetTypeException(typeName);
                }
                dataType.TypeCode = ColumnTypeCode.Udt;
                dataType.Keyspace = udtParams[0];
                dataType.Name = HexToUtf8(udtParams[1]);
                var udtInfo = new UdtColumnInfo(dataType.Keyspace + "." + dataType.Name);
                for (var i = 2; i < udtParams.Count; i++)
                {
                    var p = udtParams[i];
                    var separatorIndex = p.IndexOf(':');
                    var c = ParseFqTypeName(p, separatorIndex + 1, p.Length - (separatorIndex + 1));
                    c.Name = HexToUtf8(p.Substring(0, separatorIndex));
                    udtInfo.Fields.Add(c);
                }
                dataType.TypeInfo = udtInfo;
                return dataType;
            }
            if (typeName.IndexOf(TupleTypeName, startIndex, comparison) == startIndex)
            {
                //move cursor across the name and bypass the parenthesis
                startIndex += TupleTypeName.Length + 1;
                length -= TupleTypeName.Length + 2;
                var tupleParams = ParseParams(typeName, startIndex, length);
                if (tupleParams.Count < 1)
                {
                    //It should contain at least the keyspace, name of the udt and a type
                    throw GetTypeException(typeName);
                }
                dataType.TypeCode = ColumnTypeCode.Tuple;
                var tupleInfo = new TupleColumnInfo();
                foreach (var subTypeName in tupleParams)
                {
                    tupleInfo.Elements.Add(ParseFqTypeName(subTypeName));
                }
                dataType.TypeInfo = tupleInfo;
                return dataType;
            }
            // Assume custom type if cannot be parsed up to this point.
            dataType.TypeInfo = new CustomColumnInfo(typeName.Substring(startIndex, length));
            return dataType;
        }

        /// <summary>
        /// Parses a given CQL type name to get the data type information
        /// </summary>
        /// <exception cref="ArgumentException" />
        internal static Task<ColumnDesc> ParseTypeName(Func<string, string, Task<UdtColumnInfo>> udtResolver, string keyspace, string typeName, int startIndex = 0, int length = 0)
        {
            const StringComparison comparison = StringComparison.Ordinal;
            var dataType = new ColumnDesc
            {
                TypeCode = ColumnTypeCode.Custom
            };
            if (length == 0)
            {
                length = typeName.Length;
            }
            if (typeName.IndexOf(CqlNames.Frozen, startIndex, comparison) == startIndex)
            {
                //Remove the frozen
                startIndex += CqlNames.Frozen.Length + 1;
                length -= CqlNames.Frozen.Length + 2;
                dataType.IsFrozen = true;
            }
            if (typeName.IndexOf("'", startIndex, comparison) == startIndex)
            {
                // When quoted, this is a custom type.
                dataType.TypeInfo = new CustomColumnInfo(typeName.Substring(startIndex + 1, length - 2));
                return TaskHelper.ToTask(dataType);
            }
            if (typeName == CqlNames.Empty)
            {
                // A custom without type info
                return TaskHelper.ToTask(dataType);
            }
            if (typeName.IndexOf(CqlNames.List, startIndex, comparison) == startIndex)
            {
                //Its a list: move cursor across the name and bypass the angle brackets
                startIndex += CqlNames.List.Length + 1;
                length -= CqlNames.List.Length + 2;
                var innerTypes = ParseParams(typeName, startIndex, length, '<', '>');
                if (innerTypes.Count != 1)
                {
                    return TaskHelper.FromException<ColumnDesc>(GetTypeException(typeName));
                }
                dataType.TypeCode = ColumnTypeCode.List;
                return ParseTypeName(udtResolver, keyspace, innerTypes[0].Trim())
                    .ContinueSync(subType =>
                    {
                        dataType.TypeInfo = new ListColumnInfo
                        {
                            ValueTypeCode = subType.TypeCode,
                            ValueTypeInfo = subType.TypeInfo
                        };
                        return dataType;
                    });
            }
            if (typeName.IndexOf(CqlNames.Set, startIndex, comparison) == startIndex)
            {
                //Its a set: move cursor across the name and bypass the angle brackets
                startIndex += CqlNames.Set.Length + 1;
                length -= CqlNames.Set.Length + 2;
                var innerTypes = ParseParams(typeName, startIndex, length, '<', '>');
                if (innerTypes.Count != 1)
                {
                    return TaskHelper.FromException<ColumnDesc>(GetTypeException(typeName));
                }
                dataType.TypeCode = ColumnTypeCode.Set;
                return ParseTypeName(udtResolver, keyspace, innerTypes[0].Trim())
                    .ContinueSync(subType =>
                    {
                        dataType.TypeInfo = new SetColumnInfo
                        {
                            KeyTypeCode = subType.TypeCode,
                            KeyTypeInfo = subType.TypeInfo
                        };
                        return dataType;
                    });
            }
            if (typeName.IndexOf(CqlNames.Map, startIndex, comparison) == startIndex)
            {
                //move cursor across the name and bypass the parenthesis
                startIndex += CqlNames.Map.Length + 1;
                length -= CqlNames.Map.Length + 2;
                var innerTypes = ParseParams(typeName, startIndex, length, '<', '>');
                //It should contain the key and value types
                if (innerTypes.Count != 2)
                {
                    return TaskHelper.FromException<ColumnDesc>(GetTypeException(typeName));
                }
                dataType.TypeCode = ColumnTypeCode.Map;
                var keyTypeTask = ParseTypeName(udtResolver, keyspace, innerTypes[0].Trim());
                var valueTypeTask = ParseTypeName(udtResolver, keyspace, innerTypes[1].Trim());
                return Task.Factory.ContinueWhenAll(new[] { keyTypeTask, valueTypeTask }, tasks =>
                {
                    dataType.TypeInfo = new MapColumnInfo
                    {
                        KeyTypeCode = tasks[0].Result.TypeCode,
                        KeyTypeInfo = tasks[0].Result.TypeInfo,
                        ValueTypeCode = tasks[1].Result.TypeCode,
                        ValueTypeInfo = tasks[1].Result.TypeInfo
                    };
                    return dataType;
                });
            }
            if (typeName.IndexOf(CqlNames.Tuple, startIndex, comparison) == startIndex)
            {
                //move cursor across the name and bypass the parenthesis
                startIndex += CqlNames.Tuple.Length + 1;
                length -= CqlNames.Tuple.Length + 2;
                var tupleParams = ParseParams(typeName, startIndex, length, '<', '>');
                if (tupleParams.Count < 1)
                {
                    //It should contain at least the keyspace, name of the udt and a type
                    return TaskHelper.FromException<ColumnDesc>(GetTypeException(typeName));
                }
                dataType.TypeCode = ColumnTypeCode.Tuple;
                var elementTasks = tupleParams
                    .Select(subTypeName => ParseTypeName(udtResolver, keyspace, subTypeName.Trim()))
                    .ToArray();
                return Task.Factory.ContinueWhenAll(elementTasks, tasks =>
                {
                    dataType.TypeInfo = new TupleColumnInfo(tasks.Select(t => t.Result));
                    return dataType;
                });
            }
            if (startIndex > 0)
            {
                typeName = typeName.Substring(startIndex, length);
            }
            if (SingleCqlNames.TryGetValue(typeName, out ColumnTypeCode typeCode))
            {
                dataType.TypeCode = typeCode;
                return TaskHelper.ToTask(dataType);
            }
            typeName = typeName.Replace("\"", "");
            return udtResolver(keyspace, typeName).ContinueSync(typeInfo =>
            {
                dataType.TypeCode = ColumnTypeCode.Udt;
                dataType.TypeInfo = typeInfo ?? throw GetTypeException(typeName);
                return dataType;
            });
        }

        /// <summary>
        /// Converts a hex string to utf8 string
        /// </summary>
        private static string HexToUtf8(string hexString)
        {
            var bytes = Enumerable.Range(0, hexString.Length)
                 .Where(x => x % 2 == 0)
                 .Select(x => Convert.ToByte(hexString.Substring(x, 2), 16))
                 .ToArray();
            return Encoding.UTF8.GetString(bytes);
        }

        /// <summary>
        /// Parses comma delimited type parameters
        /// </summary>
        /// <returns></returns>
        private static List<string> ParseParams(string value, int startIndex, int length, char open = '(', char close = ')')
        {
            var types = new List<string>();
            var paramStart = startIndex;
            var level = 0;
            for (var i = startIndex; i < startIndex + length; i++)
            {
                var c = value[i];
                if (c == open)
                {
                    level++;
                }
                if (c == close)
                {
                    level--;
                }
                if (level == 0 && c == ',')
                {
                    types.Add(value.Substring(paramStart, i - paramStart));
                    paramStart = i + 1;
                }
            }
            //Add the last one
            types.Add(value.Substring(paramStart, length - (paramStart - startIndex)));
            return types;
        }

        private static Exception GetTypeException(string typeName)
        {
            return new ArgumentException(string.Format("Not a valid type {0}", typeName));
        }
    }
}
