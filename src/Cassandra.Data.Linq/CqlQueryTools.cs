//
//      Copyright (C) 2012 DataStax Inc.
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
using System.Collections;
using System.Collections.Generic;
using System.Globalization;
using System.Linq;
using System.Reflection;
using System.Text;
using System.Text.RegularExpressions;

namespace Cassandra.Data.Linq
{
    internal static class CqlQueryTools
    {
        private static readonly Regex IdentifierRx = new Regex(@"\b[a-z][a-z0-9_]*\b", RegexOptions.Compiled);

        /// <summary>
        /// Hex string lookup table.
        /// </summary>
        private static readonly string[] HexStringTable = new string[]
        {
            "00", "01", "02", "03", "04", "05", "06", "07", "08", "09", "0A", "0B", "0C", "0D", "0E", "0F",
            "10", "11", "12", "13", "14", "15", "16", "17", "18", "19", "1A", "1B", "1C", "1D", "1E", "1F",
            "20", "21", "22", "23", "24", "25", "26", "27", "28", "29", "2A", "2B", "2C", "2D", "2E", "2F",
            "30", "31", "32", "33", "34", "35", "36", "37", "38", "39", "3A", "3B", "3C", "3D", "3E", "3F",
            "40", "41", "42", "43", "44", "45", "46", "47", "48", "49", "4A", "4B", "4C", "4D", "4E", "4F",
            "50", "51", "52", "53", "54", "55", "56", "57", "58", "59", "5A", "5B", "5C", "5D", "5E", "5F",
            "60", "61", "62", "63", "64", "65", "66", "67", "68", "69", "6A", "6B", "6C", "6D", "6E", "6F",
            "70", "71", "72", "73", "74", "75", "76", "77", "78", "79", "7A", "7B", "7C", "7D", "7E", "7F",
            "80", "81", "82", "83", "84", "85", "86", "87", "88", "89", "8A", "8B", "8C", "8D", "8E", "8F",
            "90", "91", "92", "93", "94", "95", "96", "97", "98", "99", "9A", "9B", "9C", "9D", "9E", "9F",
            "A0", "A1", "A2", "A3", "A4", "A5", "A6", "A7", "A8", "A9", "AA", "AB", "AC", "AD", "AE", "AF",
            "B0", "B1", "B2", "B3", "B4", "B5", "B6", "B7", "B8", "B9", "BA", "BB", "BC", "BD", "BE", "BF",
            "C0", "C1", "C2", "C3", "C4", "C5", "C6", "C7", "C8", "C9", "CA", "CB", "CC", "CD", "CE", "CF",
            "D0", "D1", "D2", "D3", "D4", "D5", "D6", "D7", "D8", "D9", "DA", "DB", "DC", "DD", "DE", "DF",
            "E0", "E1", "E2", "E3", "E4", "E5", "E6", "E7", "E8", "E9", "EA", "EB", "EC", "ED", "EE", "EF",
            "F0", "F1", "F2", "F3", "F4", "F5", "F6", "F7", "F8", "F9", "FA", "FB", "FC", "FD", "FE", "FF"
        };

        internal static readonly DateTimeOffset UnixStart = new DateTimeOffset(1970, 1, 1, 0, 0, 0, 0, TimeSpan.Zero);

        private static readonly Dictionary<Type, string> CQLTypeNames = new Dictionary<Type, string>()
        {
            {typeof (Int32), "int"},
            {typeof (Int64), "bigint"},
            {typeof (string), "text"},
            {typeof (byte[]), "blob"},
            {typeof (Boolean), "boolean"},
            {typeof (Decimal), "decimal"},
            {typeof (Double), "double"},
            {typeof (Single), "float"},
            {typeof (Guid), "uuid"},
            {typeof (DateTimeOffset), "timestamp"},
            {typeof (DateTime), "timestamp"},
        };

        public static string CqlIdentifier(this string id)
        {
            if (!string.IsNullOrWhiteSpace(id))
            {
                if (!IdentifierRx.IsMatch(id))
                {
                    return "\"" + id.Replace("\"", "\"\"") + "\"";
                }
                else
                {
                    return id;
                }
            }
            throw new ArgumentException("invalid identifier");
        }


        public static string QuoteIdentifier(this string id)
        {
            return "\"" + id.Replace("\"", "\"\"") + "\"";
        }

        /// <summary>
        /// Returns a hex string representation of an array of bytes.
        /// http://blogs.msdn.com/b/blambert/archive/2009/02/22/blambert-codesnip-fast-byte-array-to-hex-string-conversion.aspx
        /// </summary>
        /// <param name="value">The array of bytes.</param>
        /// <returns>A hex string representation of the array of bytes.</returns>
        public static string ToHex(this byte[] value)
        {
            var stringBuilder = new StringBuilder();
            if (value != null)
            {
                foreach (byte b in value)
                {
                    stringBuilder.Append(HexStringTable[b]);
                }
            }

            return stringBuilder.ToString();
        }


        public static string Encode(this object obj)
        {
            if (obj is string) return Encode(obj as string);
            else if (obj is Boolean) return Encode((Boolean) obj);
            else if (obj is byte[]) return Encode((byte[]) obj);
            else if (obj is Double) return Encode((Double) obj);
            else if (obj is Single) return Encode((Single) obj);
            else if (obj is Decimal) return Encode((Decimal) obj);
            else if (obj is DateTimeOffset) return Encode((DateTimeOffset) obj);
                // need to treat "Unspecified" as UTC (+0) not the default behavior of DateTimeOffset which treats as Local Timezone
                // because we are about to do math against EPOCH which must align with UTC. 
                // If we don't, then the value saved will be shifted by the local timezone when retrieved back out as DateTime.
            else if (obj is DateTime)
                return Encode(((DateTime) obj).Kind == DateTimeKind.Unspecified
                                  ? new DateTimeOffset((DateTime) obj, TimeSpan.Zero)
                                  : new DateTimeOffset((DateTime) obj));
            else if (obj.GetType().IsGenericType)
            {
                if (obj.GetType().GetInterface("ISet`1") != null)
                {
                    var sb = new StringBuilder();
                    foreach (object el in (IEnumerable) obj)
                    {
                        if (sb.ToString() != "")
                            sb.Append(", ");
                        sb.Append(el.Encode());
                    }
                    return "{" + sb.ToString() + "}";
                }
                else if (obj.GetType().GetInterface("IDictionary`2") != null)
                {
                    var sb = new StringBuilder();
                    IDictionaryEnumerator enn = ((IDictionary) obj).GetEnumerator();
                    while (enn.MoveNext())
                    {
                        if (sb.ToString() != "")
                            sb.Append(", ");
                        sb.Append(enn.Key.Encode() + ":" + enn.Value.Encode());
                    }
                    return "{" + sb.ToString() + "}";
                }
                else if (obj.GetType().GetInterface("IEnumerable`1") != null)
                {
                    var sb = new StringBuilder();
                    foreach (object el in (IEnumerable) obj)
                    {
                        if (sb.ToString() != "")
                            sb.Append(", ");
                        sb.Append(el.Encode());
                    }
                    return "[" + sb.ToString() + "]";
                }
            }
            return obj.ToString();
        }


        public static string Encode(string str)
        {
            return '\'' + str.Replace("\'", "\'\'") + '\'';
        }

        public static string Encode(bool val)
        {
            return (val ? "true" : "false");
        }

        public static string Encode(byte[] val)
        {
            return "0x" + val.ToHex();
        }

        public static string Encode(Double val)
        {
            return val.ToString(new CultureInfo("en-US"));
        }

        public static string Encode(Single val)
        {
            return val.ToString(new CultureInfo("en-US"));
        }

        public static string Encode(Decimal val)
        {
            return val.ToString(new CultureInfo("en-US"));
        }

        public static string Encode(DateTimeOffset val)
        {
            if (val == DateTimeOffset.MinValue)
                return 0.ToString(CultureInfo.InvariantCulture);
            else
                return Convert.ToInt64(Math.Floor((val - UnixStart).TotalMilliseconds)).ToString(CultureInfo.InvariantCulture);
        }

        private static string GetCqlTypeFromType(Type tpy)
        {
            if (CQLTypeNames.ContainsKey(tpy))
                return CQLTypeNames[tpy];
            else
            {
                if (tpy.IsGenericType)
                {
                    if (tpy.Name.Equals("Nullable`1"))
                    {
                        return GetCqlTypeFromType(tpy.GetGenericArguments()[0]);
                    }
                    else if (tpy.GetInterface("ISet`1") != null)
                    {
                        return "set<" + GetCqlTypeFromType(tpy.GetGenericArguments()[0]) + ">";
                    }
                    else if (tpy.GetInterface("IDictionary`2") != null)
                    {
                        return "map<" + GetCqlTypeFromType(tpy.GetGenericArguments()[0]) + ", " + GetCqlTypeFromType(tpy.GetGenericArguments()[1]) +
                               ">";
                    }
                    else if (tpy.GetInterface("IEnumerable`1") != null)
                    {
                        return "list<" + GetCqlTypeFromType(tpy.GetGenericArguments()[0]) + ">";
                    }
                }
                else if (tpy.Name == "BigDecimal")
                    return "decimal";
            }

            var supportedTypes = new StringBuilder();
            foreach (Type tn in CQLTypeNames.Keys)
                supportedTypes.Append(tn.FullName + ", ");
            supportedTypes.Append(", their nullable counterparts, and implementations of IEnumerable<T>, IDictionary<K,V>");

            throw new ArgumentException("Unsupported datatype " + tpy.Name + ". Supported are: " + supportedTypes.ToString() + ".");
        }

        internal static string CalculateMemberName(MemberInfo prop)
        {
            string memName = prop.Name;
            var propNameAttr = prop.GetCustomAttributes(typeof (ColumnAttribute), false).FirstOrDefault() as ColumnAttribute;
            if (propNameAttr != null && !string.IsNullOrEmpty(propNameAttr.Name))
                memName = propNameAttr.Name;
            return memName;
        }

        public static List<string> GetCreateCQL(ITable table, bool ifNotExists)
        {
            var commands = new List<string>();
            var sb = new StringBuilder();
            int countersCount = 0;
            bool countersSpotted = false;
            sb.Append("CREATE TABLE ");
            if (ifNotExists)
                sb.Append("IF NOT EXISTS ");
            sb.Append(table.GetQuotedTableName());
            sb.Append("(");
            string crtIndex = "CREATE INDEX " + (ifNotExists ? "IF NOT EXISTS " : "") + "ON " + table.GetQuotedTableName() + "(";
            string crtIndexAll = string.Empty;

            var clusteringKeys = new SortedDictionary<int, ClusteringKeyAttribute>();
            var partitionKeys = new SortedDictionary<int, string>();
            var directives = new List<string>();

            List<MemberInfo> props = table.GetEntityType().GetPropertiesOrFields();
            int curLevel = 0;

            if (table.GetEntityType().GetCustomAttributes(typeof (CompactStorageAttribute), false).Any())
                directives.Add("COMPACT STORAGE");

            foreach (MemberInfo prop in props)
            {
                Type tpy = prop.GetTypeFromPropertyOrField();

                string memName = CalculateMemberName(prop);

                sb.Append(memName.QuoteIdentifier());
                sb.Append(" ");

                if (prop.GetCustomAttributes(typeof (CounterAttribute), true).FirstOrDefault() as CounterAttribute != null)
                {
                    countersCount++;
                    countersSpotted = true;
                    if (prop.GetCustomAttributes(typeof (ClusteringKeyAttribute), true).FirstOrDefault() as ClusteringKeyAttribute != null ||
                        prop.GetCustomAttributes(typeof (PartitionKeyAttribute), true).FirstOrDefault() as PartitionKeyAttribute != null)
                        throw new InvalidQueryException("Counter can not be a part of PRIMARY KEY !");
                    if (tpy != typeof (Int64))
                        throw new InvalidQueryException("Counters can be only of Int64(long) type !");
                    else
                        sb.Append("counter");
                }
                else
                    sb.Append(GetCqlTypeFromType(tpy));

                sb.Append(", ");
                var pk = prop.GetCustomAttributes(typeof (PartitionKeyAttribute), true).FirstOrDefault() as PartitionKeyAttribute;
                if (pk != null)
                {
                    int idx = pk.Index;
                    if (idx == -1)
                        idx = curLevel++;
                    partitionKeys.Add(idx, memName);
                }
                else
                {
                    var rk = prop.GetCustomAttributes(typeof (ClusteringKeyAttribute), true).FirstOrDefault() as ClusteringKeyAttribute;
                    if (rk != null)
                    {
                        int idx = rk.Index;
                        if (idx == -1)
                            idx = curLevel++;
                        rk.Name = memName;
                        clusteringKeys.Add(idx, rk);
                    }
                }
                var si = prop.GetCustomAttributes(typeof (SecondaryIndexAttribute), true).FirstOrDefault() as SecondaryIndexAttribute;
                if (si != null)
                {
                    commands.Add(crtIndex + memName.QuoteIdentifier() + ");");
                }
            }

            foreach (KeyValuePair<int, ClusteringKeyAttribute> clustKey in clusteringKeys)
                if (clustKey.Value.ClusteringOrder != null)
                    directives.Add(string.Format("CLUSTERING ORDER BY ({0} {1})", (string) clustKey.Value.Name.QuoteIdentifier(),
                                                 clustKey.Value.ClusteringOrder));
                else
                    break;

            if (countersSpotted) // validating if table consists only of counters
                if (countersCount + clusteringKeys.Count + 1 != props.Count())
                    throw new InvalidQueryException("Counter table can consist only of counters.");

            sb.Append("PRIMARY KEY(");
            if (partitionKeys.Count > 1)
                sb.Append("(");
            bool fisrtParKey = true;
            foreach (KeyValuePair<int, string> kv in partitionKeys)
            {
                if (!fisrtParKey)
                    sb.Append(", ");
                else
                    fisrtParKey = false;
                sb.Append(kv.Value.QuoteIdentifier());
            }
            if (partitionKeys.Count > 1)
                sb.Append(")");
            foreach (KeyValuePair<int, ClusteringKeyAttribute> kv in clusteringKeys)
            {
                sb.Append(", ");
                sb.Append(kv.Value.Name.QuoteIdentifier());
            }
            sb.Append("))");

            if (directives.Count > 0)
            {
                sb.Append(" WITH ");
                bool first = true;
                foreach (string par in directives)
                {
                    sb.Append((first ? "" : " AND ") + par);
                    first = false;
                }
            }

            sb.Append(";");

            commands.Add(sb.ToString());
            if (commands.Count > 1)
                commands.Reverse();
            return commands;
        }

        public static string GetInsertCQLAndValues(object row, string quotedtablename, out object[] values, int? ttl, DateTimeOffset? timestamp,
                                                   bool ifNotExists, bool withValues = true)
        {
            var cqlTool = new CqlStringTool();
            Type rowType = row.GetType();
            var sb = new StringBuilder();
            sb.Append("INSERT INTO ");
            sb.Append(quotedtablename);
            sb.Append("(");

            List<MemberInfo> props = rowType.GetPropertiesOrFields();
            bool first = true;
            foreach (MemberInfo prop in props)
            {
                object val = prop.GetValueFromPropertyOrField(row);
                if (val == null) continue;
                if (first) first = false;
                else sb.Append(", ");
                string memName = CalculateMemberName(prop);
                sb.Append(memName.QuoteIdentifier());
            }
            sb.Append(") VALUES (");
            first = true;
            foreach (MemberInfo prop in props)
            {
                object val = prop.GetValueFromPropertyOrField(row);
                if (val == null) continue;
                if (first) first = false;
                else sb.Append(", ");
                sb.Append(cqlTool.AddValue(val));
            }
            sb.Append(")");
            if (ifNotExists)
            {
                sb.Append(" IF NOT EXISTS");
            }
            if (ttl != null || timestamp != null)
            {
                sb.Append(" USING");
            }
            if (ttl != null)
            {
                sb.Append(" TTL ");
                sb.Append(ttl.Value);
                if (timestamp != null)
                    sb.Append(" AND");
            }
            if (timestamp != null)
            {
                sb.Append(" TIMESTAMP ");
                sb.Append((timestamp.Value - CqlQueryTools.UnixStart).Ticks / 10);
            }

            if (withValues)
                return cqlTool.FillWithValues(sb.ToString(), out values);
            else
            {
                values = null;
                return cqlTool.FillWithEncoded(sb.ToString());
            }
        }

        public static string GetUpdateCQLAndValues(object row, object newRow, string quotedtablename, out object[] values, bool all = false,
                                                   bool withValues = true)
        {
            var cqlTool = new CqlStringTool();
            Type rowType = row.GetType();
            var set = new StringBuilder();
            var where = new StringBuilder();
            List<MemberInfo> props = rowType.GetPropertiesOrFields();
            bool firstSet = true;
            bool firstWhere = true;
            bool changeDetected = false;
            foreach (MemberInfo prop in props)
            {
                string memName = CalculateMemberName(prop);
                var pk = prop.GetCustomAttributes(typeof (PartitionKeyAttribute), true).FirstOrDefault() as PartitionKeyAttribute;
                if (pk == null)
                {
                    var rk = prop.GetCustomAttributes(typeof (ClusteringKeyAttribute), true).FirstOrDefault() as ClusteringKeyAttribute;
                    if (rk == null)
                    {
                        var counter = prop.GetCustomAttributes(typeof (CounterAttribute), true).FirstOrDefault() as CounterAttribute;
                        if (counter != null)
                        {
                            long diff = (Int64) prop.GetValueFromPropertyOrField(newRow) - (Int64) prop.GetValueFromPropertyOrField(row);
                            if (diff != 0 || (Int64) prop.GetValueFromPropertyOrField(newRow) == 0)
                            {
                                changeDetected = true;
                                if (firstSet) firstSet = false;
                                else set.Append(", ");
                                set.Append(memName.QuoteIdentifier() + " = " + memName.QuoteIdentifier());
                                set.Append((diff >= 0) ? "+" + diff : diff.ToString(CultureInfo.InvariantCulture));
                            }
                            continue;
                        }
                        else
                        {
                            object newVal = prop.GetValueFromPropertyOrField(newRow);
                            if (newVal != null)
                            {
                                bool areDifferent = !newVal.Equals(prop.GetValueFromPropertyOrField(row));
                                if (all || (areDifferent))
                                {
                                    if (areDifferent)
                                        changeDetected = true;
                                    if (firstSet) firstSet = false;
                                    else set.Append(", ");
                                    set.Append(memName.QuoteIdentifier());
                                    set.Append(" = " + cqlTool.AddValue(newVal) + " ");
                                }
                            }
                            else
                            {
                                changeDetected = true;
                                if (firstSet) firstSet = false;
                                else set.Append(", ");
                                set.Append(memName.QuoteIdentifier());
                                set.Append(" = NULL ");
                            }
                            continue;
                        }
                    }
                }

                object pv = prop.GetValueFromPropertyOrField(row);
                if (pv != null)
                {
                    if (firstWhere) firstWhere = false;
                    else where.Append(" AND ");
                    where.Append(memName.QuoteIdentifier());
                    where.Append(" = " + cqlTool.AddValue(pv) + " ");
                }
            }

            values = null;

            if (!changeDetected)
                return null;

            var sb = new StringBuilder();
            sb.Append("UPDATE ");
            sb.Append(quotedtablename);
            sb.Append(" SET ");
            sb.Append(set);
            sb.Append(" WHERE ");
            sb.Append(where);

            if (withValues)
                return cqlTool.FillWithValues(sb.ToString(), out values);
            else
            {
                values = null;
                return cqlTool.FillWithEncoded(sb.ToString());
            }
        }

        public static string GetDeleteCQLAndValues(object row, string quotedtablename, out object[] values, bool withValues = true)
        {
            var cqlTool = new CqlStringTool();
            Type rowType = row.GetType();

            var sb = new StringBuilder();
            sb.Append("DELETE FROM ");
            sb.Append(quotedtablename);
            sb.Append(" WHERE ");

            List<MemberInfo> props = rowType.GetPropertiesOrFields();
            bool first = true;
            foreach (MemberInfo prop in props)
            {
                var pk = prop.GetCustomAttributes(typeof (PartitionKeyAttribute), true).FirstOrDefault() as PartitionKeyAttribute;
                if (pk == null)
                {
                    var rk = prop.GetCustomAttributes(typeof (ClusteringKeyAttribute), true).FirstOrDefault() as ClusteringKeyAttribute;
                    if (rk == null)
                    {
                        continue;
                    }
                }
                object pv = prop.GetValueFromPropertyOrField(row);
                if (pv != null)
                {
                    if (first) first = false;
                    else sb.Append(" AND ");
                    string memName = CalculateMemberName(prop);
                    sb.Append(memName.QuoteIdentifier());
                    sb.Append(" =  " + cqlTool.AddValue(pv) + " ");
                }
            }

            if (withValues)
                return cqlTool.FillWithValues(sb.ToString(), out values);
            else
            {
                values = null;
                return cqlTool.FillWithEncoded(sb.ToString());
            }
        }

        public static T GetRowFromCqlRow<T>(Row cqlRow, Dictionary<string, int> colToIdx, Dictionary<string, Tuple<string, object, int>> mappings,
                                            Dictionary<string, string> alter)
        {
            ConstructorInfo ncstr = typeof (T).GetConstructor(new Type[] {});
            if (ncstr != null)
            {
                var row = (T) ncstr.Invoke(new object[] {});

                MemberInfo[] props = typeof (T).GetMembers();
                foreach (MemberInfo prop in props)
                {
                    if (prop is FieldInfo || prop is PropertyInfo)
                    {
                        int idx;

                        string propName = prop.Name;
                        if (alter.ContainsKey(propName))
                            propName = alter[propName];

                        if (colToIdx.ContainsKey(propName))
                            idx = colToIdx[propName];
                        else if (mappings.ContainsKey(propName) && mappings[propName].Item1 == null)
                        {
                            prop.SetValueFromPropertyOrField(row, mappings[propName].Item2);
                            continue;
                        }
                        else if (mappings.ContainsKey(propName) && colToIdx.ContainsKey(alter[mappings[propName].Item1]))
                            idx = colToIdx[alter[mappings[propName].Item1]];
                        else
                            continue;
                        object val = cqlRow.GetValue(prop.GetTypeFromPropertyOrField(), idx);
                        if (val == null)
                            prop.SetValueFromPropertyOrField(row, val);
                        else
                        {
                            Type tpy = (prop is FieldInfo) ? (prop as FieldInfo).FieldType : (prop as PropertyInfo).PropertyType;

                            if (tpy.IsGenericType && !tpy.Name.Equals("Nullable`1"))
                            {
                                if (tpy.GetInterface("IDictionary`2") != null)
                                {
                                    Type openType = typeof (IDictionary<,>);
                                    Type dictType = openType.MakeGenericType(tpy.GetGenericArguments()[0], tpy.GetGenericArguments()[1]);
                                    ConstructorInfo dt = tpy.GetConstructor(new Type[] {dictType});
                                    prop.SetValueFromPropertyOrField(row, dt.Invoke(new object[] {val}));
                                }
                                else if (tpy.GetInterface("IEnumerable`1") != null)
                                {
                                    Type openType = typeof (IEnumerable<>);
                                    Type listType = openType.MakeGenericType(tpy.GetGenericArguments().First());
                                    ConstructorInfo dt = tpy.GetConstructor(new Type[] {listType});
                                    prop.SetValueFromPropertyOrField(row, dt.Invoke(new object[] {val}));
                                }
                                else
                                    throw new InvalidOperationException();
                            }
                            else
                                prop.SetValueFromPropertyOrField(row, val);
                        }
                    }
                }
                return row;
            }
            else
            {
                if (cqlRow.Length == 1 &&
                    (typeof (T).IsPrimitive || typeof (T) == typeof (Decimal) || typeof (T) == typeof (string) || typeof (T) == typeof (byte[]) ||
                     typeof (T) == typeof (Guid)))
                {
                    return (T) cqlRow[0];
                }
                else
                {
                    ConstructorInfo ocstr = typeof (T).GetConstructor(new Type[] {typeof (T)});
                    if (ocstr != null && cqlRow.Length == 1)
                    {
                        return (T) Activator.CreateInstance(typeof (T), cqlRow[0]);
                    }
                    else
                    {
                        var objs = new object[mappings.Count];
                        MemberInfo[] props = typeof (T).GetMembers();
                        int idx = 0;
                        foreach (MemberInfo prop in props)
                        {
                            if (prop is PropertyInfo || prop is FieldInfo)
                            {
                                if (mappings[prop.Name].Item1 == null)
                                {
                                    objs[mappings[prop.Name].Item3] = mappings[prop.Name].Item2;
                                }
                                else
                                {
                                    object val = cqlRow.GetValue(prop.GetTypeFromPropertyOrField(), idx);
                                    objs[mappings[prop.Name].Item3] = val;
                                    idx++;
                                }
                            }
                        }
                        return (T) Activator.CreateInstance(typeof (T), objs);
                    }
                }
            }
        }
    }
}
