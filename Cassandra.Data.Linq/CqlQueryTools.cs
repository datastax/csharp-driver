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
﻿using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Text.RegularExpressions;
using System.Globalization;
using System.Reflection;
using System.Collections;

namespace Cassandra.Data.Linq
{
    internal static class ReflExt
    {
        [ThreadStatic]
        static Dictionary<Type,List<MemberInfo>> ReflexionCachePF=null;

        public static List<MemberInfo> GetPropertiesOrFields(this Type tpy)
        {
            if (ReflexionCachePF == null)
                ReflexionCachePF = new Dictionary<Type, List<MemberInfo>>();

            List<MemberInfo> val;
            if (ReflexionCachePF.TryGetValue(tpy, out val))
                return val;

            List<MemberInfo> ret = new List<MemberInfo>();
            var props = tpy.GetMembers();
            foreach (var prop in props)
            {
                if (prop is PropertyInfo || prop is FieldInfo)
                    ret.Add(prop);
            }
            ReflexionCachePF.Add(tpy, ret);
            return ret;
        }

        public static object GetValueFromPropertyOrField(this MemberInfo prop, object x)
        {
            if (prop is PropertyInfo)
                return (prop as PropertyInfo).GetValue(x, null);
            else if (prop is FieldInfo)
                return (prop as FieldInfo).GetValue(x);
            else
                throw new InvalidOperationException();
        }

        public static Type GetTypeFromPropertyOrField(this MemberInfo prop)
        {
            if (prop is PropertyInfo)
                return (prop as PropertyInfo).PropertyType;
            else if (prop is FieldInfo)
                return (prop as FieldInfo).FieldType;
            else
                throw new InvalidOperationException();
        }

        public static void SetValueFromPropertyOrField(this MemberInfo prop, object x, object v)
        {
            if (prop is PropertyInfo)
                (prop as PropertyInfo).SetValue(x, v, null);
            else if (prop is FieldInfo)
                (prop as FieldInfo).SetValue(x, v);
            else
                throw new InvalidOperationException();
        }
    
    }

    internal static class CqlQueryTools
    {
        static readonly Regex IdentifierRx = new Regex(@"\b[a-z][a-z0-9_]*\b", RegexOptions.Compiled);

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
            else if (obj is Boolean) return Encode((Boolean)obj);
            else if (obj is byte[]) return Encode((byte[])obj);
            else if (obj is Double) return Encode((Double)obj);
            else if (obj is Single) return Encode((Single)obj);
            else if (obj is Decimal) return Encode((Decimal)obj);
            else if (obj is DateTimeOffset) return Encode((DateTimeOffset)obj);
            // need to treat "Unspecified" as UTC (+0) not the default behavior of DateTimeOffset which treats as Local Timezone
            // because we are about to do math against EPOCH which must align with UTC. 
            // If we don't, then the value saved will be shifted by the local timezone when retrieved back out as DateTime.
            else if (obj is DateTime) return Encode(((DateTime)obj).Kind == DateTimeKind.Unspecified 
                    ? new DateTimeOffset((DateTime)obj,TimeSpan.Zero) 
                    : new DateTimeOffset((DateTime)obj));
            else if (obj.GetType().IsGenericType)
            {
                if (obj.GetType().GetInterface("ISet`1") != null)
                {
                    var sb = new StringBuilder();
                    foreach (var el in (IEnumerable)obj)
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
                    IDictionaryEnumerator enn = ((IDictionary)obj).GetEnumerator();
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
                    foreach (var el in (IEnumerable)obj)
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
            if (val == DateTimeOffset.MinValue) return
                0.ToString();
            else
                return Convert.ToInt64(Math.Floor((val - UnixStart).TotalMilliseconds)).ToString();
        }

        internal static readonly DateTimeOffset UnixStart = new DateTimeOffset(1970, 1, 1, 0, 0, 0, 0, TimeSpan.Zero);

        static readonly Dictionary<Type, string> CQLTypeNames = new Dictionary<Type, string>() {
        { typeof(Int32), "int" }, 
        { typeof(Int64), "bigint" }, 
        { typeof(string), "text" }, 
        { typeof(byte[]), "blob" },
        { typeof(Boolean), "boolean" },
        { typeof(Decimal), "decimal" },
        { typeof(Double), "double" },
        { typeof(Single), "float" },
        { typeof(Guid), "uuid" },
        { typeof(DateTimeOffset), "timestamp" },
        { typeof(DateTime), "timestamp" },
        };

        static string GetCqlTypeFromType(Type tpy)
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
                        return "map<" + GetCqlTypeFromType(tpy.GetGenericArguments()[0]) + ", " + GetCqlTypeFromType(tpy.GetGenericArguments()[1]) + ">";
                    }
                    else if (tpy.GetInterface("IEnumerable`1") != null)
                    {
                        return "list<" + GetCqlTypeFromType(tpy.GetGenericArguments()[0]) + ">";
                    }
                }
                else
                    if (tpy.Name == "BigDecimal")
                        return "decimal";
            }

            StringBuilder supportedTypes = new StringBuilder();
            foreach (var tn in CQLTypeNames.Keys)
                supportedTypes.Append(tn.FullName + ", ");
            supportedTypes.Append(", their nullable counterparts, and implementations of IEnumerable<T>, IDictionary<K,V>");

            throw new ArgumentException("Unsupported datatype " + tpy.Name + ". Supported are: " + supportedTypes.ToString() + ".");
        }

        internal static string CalculateMemberName(MemberInfo prop)
        {
            var memName = prop.Name;
            var propNameAttr = prop.GetCustomAttributes(typeof(ColumnAttribute), false).FirstOrDefault() as ColumnAttribute;
            if (propNameAttr != null && !string.IsNullOrEmpty(propNameAttr.Name))
                memName = propNameAttr.Name;
            return memName;
        }

        public static List<string> GetCreateCQL(ITable table)
        {
            var commands = new List<string>();
            var sb = new StringBuilder();
            int countersCount = 0;
            bool countersSpotted = false;
            sb.Append("CREATE TABLE ");
            sb.Append(table.GetQuotedTableName());
            sb.Append("(");
            string crtIndex = "CREATE INDEX ON " + table.GetQuotedTableName() + "(";
            string crtIndexAll = string.Empty;
             
            var clusteringKeys = new SortedDictionary<int, ClusteringKeyAttribute>();
            var partitionKeys = new SortedDictionary<int, string>();
            var directives = new List<string>(); 

            var props = table.GetEntityType().GetPropertiesOrFields();
            int curLevel = 0;

            if (table.GetEntityType().GetCustomAttributes(typeof(CompactStorageAttribute), false).Any())
                directives.Add("COMPACT STORAGE");

            foreach (var prop in props)
            {
                Type tpy = prop.GetTypeFromPropertyOrField();

                var memName = CalculateMemberName(prop);

                sb.Append(memName.QuoteIdentifier());
                sb.Append(" ");

                if (prop.GetCustomAttributes(typeof(CounterAttribute), true).FirstOrDefault() as CounterAttribute != null)
                {
                    countersCount++;
                    countersSpotted = true;
                    if (prop.GetCustomAttributes(typeof(ClusteringKeyAttribute), true).FirstOrDefault() as ClusteringKeyAttribute != null || prop.GetCustomAttributes(typeof(PartitionKeyAttribute), true).FirstOrDefault() as PartitionKeyAttribute != null)
                        throw new InvalidQueryException("Counter can not be a part of PRIMARY KEY !");
                    if (tpy != typeof(Int64))
                        throw new InvalidQueryException("Counters can be only of Int64(long) type !");
                    else
                        sb.Append("counter");
                }
                else
                    sb.Append(GetCqlTypeFromType(tpy));

                sb.Append(", ");
                var pk = prop.GetCustomAttributes(typeof(PartitionKeyAttribute), true).FirstOrDefault() as PartitionKeyAttribute;
                if (pk != null)
                {
                    var idx = pk.Index;
                    if (idx == -1)
                        idx = curLevel++;
                    partitionKeys.Add(idx, memName);
                }
                else
                {
                    var rk = prop.GetCustomAttributes(typeof(ClusteringKeyAttribute), true).FirstOrDefault() as ClusteringKeyAttribute;
                    if (rk != null)
                    {
                        var idx = rk.Index;
                        if (idx == -1)
                            idx = curLevel++;
                        rk.Name = memName;
                        clusteringKeys.Add(idx, rk);                                        
                    }
                    else
                    {
                        var si = prop.GetCustomAttributes(typeof(SecondaryIndexAttribute), true).FirstOrDefault() as SecondaryIndexAttribute;
                        if (si != null)
                        {
                            commands.Add(crtIndex + memName.QuoteIdentifier() + ");");
                        }
                    }
                }
            }

            foreach (var clustKey in clusteringKeys)
                if (clustKey.Value.ClusteringOrder != null)
                    directives.Add(string.Format("CLUSTERING ORDER BY ({0} {1})", (string)clustKey.Value.Name.QuoteIdentifier(), clustKey.Value.ClusteringOrder));
                else
                    break;

            if (countersSpotted)// validating if table consists only of counters
                if (countersCount + clusteringKeys.Count + 1 != props.Count())
                    throw new InvalidQueryException("Counter table can consist only of counters.");

            sb.Append("PRIMARY KEY(");
            if (partitionKeys.Count > 1)
                sb.Append("(");
            bool fisrtParKey = true;
            foreach (var kv in partitionKeys)
            {
                if (!fisrtParKey)
                    sb.Append(", ");
                else
                    fisrtParKey = false;
                sb.Append(kv.Value.QuoteIdentifier());
            }
            if (partitionKeys.Count > 1)
                sb.Append(")");
            foreach (var kv in clusteringKeys)
            {
                sb.Append(", ");
                sb.Append(kv.Value.Name.QuoteIdentifier());
            }
            sb.Append("))");

            if (directives.Count > 0)
            {
                sb.Append(" WITH ");
                bool first = true;
                foreach (var par in directives)
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

        public static string GetInsertCQL(object row, string quotedtablename, int? ttl, DateTimeOffset? timestamp)
        {
            var rowType = row.GetType();
            var sb = new StringBuilder();
            sb.Append("INSERT INTO ");
            sb.Append(quotedtablename);
            sb.Append("(");

            var props = rowType.GetPropertiesOrFields();
            bool first = true;
            foreach (var prop in props)
            {
                var val = prop.GetValueFromPropertyOrField(row);
                if (val == null) continue;
                if (first) first = false; else sb.Append(", ");
                var memName = CalculateMemberName(prop);
                sb.Append(memName.QuoteIdentifier());
            }
            sb.Append(") VALUES (");
            first = true;
            foreach (var prop in props)
            {
                var val = prop.GetValueFromPropertyOrField(row);
                if (val == null) continue;
                if (first) first = false; else sb.Append(", ");
                sb.Append(val.Encode());
            }
            sb.Append(")");
            if (ttl != null || timestamp != null)
            {
                sb.Append(" USING ");
            }
            if (ttl != null)
            {
                sb.Append("TTL ");
                sb.Append(ttl.Value);
                if (timestamp != null)
                    sb.Append(" AND ");
            }
            if (timestamp != null)
            {
                sb.Append("TIMESTAMP ");
                sb.Append((timestamp.Value - CqlQueryTools.UnixStart).Ticks / 10);
            }
            return sb.ToString();
        }

        public static string GetUpdateCQL(object row, object newRow, string quotedtablename,  bool all = false)
        {
            var rowType = row.GetType();
            var set = new StringBuilder();
            var where = new StringBuilder();
            var props = rowType.GetPropertiesOrFields();
            bool firstSet = true;
            bool firstWhere = true;
            bool changeDetected = false;
            foreach (var prop in props)
            {
                var memName = CalculateMemberName(prop);
                var pk = prop.GetCustomAttributes(typeof(PartitionKeyAttribute), true).FirstOrDefault() as PartitionKeyAttribute;
                if (pk == null)
                {
                    var rk = prop.GetCustomAttributes(typeof(ClusteringKeyAttribute), true).FirstOrDefault() as ClusteringKeyAttribute;
                    if (rk == null)
                    {
                        var counter = prop.GetCustomAttributes(typeof(CounterAttribute), true).FirstOrDefault() as CounterAttribute;
                        if (counter != null)
                        {
                            var diff = (Int64)prop.GetValueFromPropertyOrField(newRow) - (Int64)prop.GetValueFromPropertyOrField(row);
                            if (diff != 0 || (Int64)prop.GetValueFromPropertyOrField(newRow) == 0)
                            {
                                changeDetected = true;
                                if (firstSet) firstSet = false; else set.Append(", ");
                                set.Append(memName.QuoteIdentifier() + " = " + memName.QuoteIdentifier());
                                set.Append((diff >= 0) ? "+" + diff.ToString() : diff.ToString());
                            }
                            continue;
                        }
                        else
                        {
                            var newVal = prop.GetValueFromPropertyOrField(newRow);
                            if (newVal != null)
                            {
                                bool areDifferent = !newVal.Equals(prop.GetValueFromPropertyOrField(row));
                                if (all || (areDifferent))
                                {
                                    if (areDifferent)
                                        changeDetected = true;
                                    if (firstSet) firstSet = false; else set.Append(", ");
                                    set.Append(memName.QuoteIdentifier());
                                    set.Append(" = ");
                                    set.Append(Encode(newVal));
                                }
                            }
                            continue;
                        }
                    }
                }

                var pv = prop.GetValueFromPropertyOrField(row);
                if (pv != null)
                {
                    if (firstWhere) firstWhere = false; else where.Append(" AND ");
                    where.Append(memName.QuoteIdentifier());
                    where.Append(" = ");
                    where.Append(Encode(pv));
                }
            }

            if (!changeDetected)
                return null;

            var sb = new StringBuilder();
            sb.Append("UPDATE ");
            sb.Append(quotedtablename);
            sb.Append(" SET ");
            sb.Append(set);
            sb.Append(" WHERE ");
            sb.Append(where);
 
            return sb.ToString();
        }

        public static string GetDeleteCQL(object row, string quotedtablename)
        {
            var rowType = row.GetType();

            var sb = new StringBuilder();
            sb.Append("DELETE FROM ");
            sb.Append(quotedtablename);
            sb.Append(" WHERE ");

            var props = rowType.GetPropertiesOrFields();
            bool first = true;
            foreach (var prop in props)
            {
                var pk = prop.GetCustomAttributes(typeof(PartitionKeyAttribute), true).FirstOrDefault() as PartitionKeyAttribute;
                if (pk == null)
                {
                    var rk = prop.GetCustomAttributes(typeof(ClusteringKeyAttribute), true).FirstOrDefault() as ClusteringKeyAttribute;
                    if (rk == null)
                    {
                        continue;
                    }
                }
                var pv = prop.GetValueFromPropertyOrField(row);
                if (pv != null)
                {
                    if (first) first = false; else sb.Append(" AND ");
                    var memName = CalculateMemberName(prop);
                    sb.Append(memName.QuoteIdentifier());
                    sb.Append(" = ");
                    sb.Append(Encode(pv));
                }
            }
            return sb.ToString();
        }

        public static T GetRowFromCqlRow<T>(Row cqlRow, Dictionary<string, int> colToIdx, Dictionary<string, Tuple<string, object, int>> mappings, Dictionary<string, string> alter)
        {
            var ncstr = typeof(T).GetConstructor(new Type[] { });
            if (ncstr != null)
            {
                var row = (T)ncstr.Invoke(new object[] { });

                var props = typeof(T).GetMembers();
                foreach (var prop in props)
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
                        var val = cqlRow.GetValue(prop.GetTypeFromPropertyOrField(), idx);
                        if (val == null)
                            prop.SetValueFromPropertyOrField(row, val);
                        else
                        {
                            Type tpy = (prop is FieldInfo) ? (prop as FieldInfo).FieldType : (prop as PropertyInfo).PropertyType;

                            if (tpy.IsGenericType && !tpy.Name.Equals("Nullable`1"))
                            {
                                if (tpy.GetInterface("IDictionary`2") != null)
                                {
                                    var openType = typeof(IDictionary<,>);
                                    var dictType = openType.MakeGenericType(tpy.GetGenericArguments()[0], tpy.GetGenericArguments()[1]);
                                    var dt = tpy.GetConstructor(new Type[] { dictType });
                                    prop.SetValueFromPropertyOrField(row, dt.Invoke(new object[] { val }));
                                }
                                else
                                    if (tpy.GetInterface("IEnumerable`1") != null)
                                    {
                                        var openType = typeof(IEnumerable<>);
                                        var listType = openType.MakeGenericType(tpy.GetGenericArguments().First());
                                        var dt = tpy.GetConstructor(new Type[] { listType });
                                        prop.SetValueFromPropertyOrField(row, dt.Invoke(new object[] { val }));
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
                if (cqlRow.Length == 1 && (typeof(T).IsPrimitive || typeof(T) == typeof(Decimal) || typeof(T) == typeof(string) || typeof(T) == typeof(byte[]) || typeof(T) == typeof(Guid)))
                {
                    return (T)cqlRow[0];
                }
                else
                {
                    var ocstr = typeof(T).GetConstructor(new Type[] { typeof(T) });
                    if (ocstr != null && cqlRow.Length == 1)
                    {
                        return (T)Activator.CreateInstance(typeof(T), cqlRow[0]);
                    }
                    else
                    {
                        var objs = new object[mappings.Count];
                        var props = typeof(T).GetMembers();
                        int idx = 0;
                        foreach (var prop in props)
                        {
                            if (prop is PropertyInfo || prop is FieldInfo)
                            {
                                if (mappings[prop.Name].Item1 == null)
                                {
                                    objs[mappings[prop.Name].Item3] = mappings[prop.Name].Item2;
                                }
                                else
                                {
                                    var val = cqlRow.GetValue(prop.GetTypeFromPropertyOrField(), idx);
                                    objs[mappings[prop.Name].Item3] = val;
                                    idx++;
                                }
                            }
                        }
                        return (T)Activator.CreateInstance(typeof(T), objs);
                    }
                }
            }
        }
    }

    internal class CqlMthHelps
    {
        static CqlMthHelps _instance = new CqlMthHelps();
        internal static MethodInfo SelectMi = typeof(CqlMthHelps).GetMethod("Select", BindingFlags.NonPublic | BindingFlags.Static);
        internal static MethodInfo WhereMi = typeof(CqlMthHelps).GetMethod("Where", BindingFlags.NonPublic | BindingFlags.Static);
        internal static MethodInfo FirstMi = typeof(CqlMthHelps).GetMethod("First", BindingFlags.NonPublic | BindingFlags.Static);
        internal static MethodInfo First_ForCQLTableMi = typeof(CqlMthHelps).GetMethod("First", new Type[] { typeof(ITable), typeof(int), typeof(object) });        
        internal static MethodInfo FirstOrDefaultMi = typeof(CqlMthHelps).GetMethod("FirstOrDefault", BindingFlags.NonPublic | BindingFlags.Static);
        internal static MethodInfo FirstOrDefault_ForCQLTableMi = typeof(CqlMthHelps).GetMethod("FirstOrDefault", new Type[] { typeof(ITable), typeof(int), typeof(object) }); 
        internal static MethodInfo TakeMi = typeof(CqlMthHelps).GetMethod("Take", BindingFlags.NonPublic | BindingFlags.Static);
        internal static MethodInfo CountMi = typeof(CqlMthHelps).GetMethod("Count", BindingFlags.NonPublic | BindingFlags.Static);
        internal static MethodInfo OrderByMi = typeof(CqlMthHelps).GetMethod("OrderBy", BindingFlags.NonPublic | BindingFlags.Static);
        internal static MethodInfo OrderByDescendingMi = typeof(CqlMthHelps).GetMethod("OrderByDescending", BindingFlags.NonPublic | BindingFlags.Static);
        internal static MethodInfo ThenByMi = typeof(CqlMthHelps).GetMethod("ThenBy", BindingFlags.NonPublic | BindingFlags.Static);
        internal static MethodInfo ThenByDescendingMi = typeof(CqlMthHelps).GetMethod("ThenByDescending", BindingFlags.NonPublic | BindingFlags.Static);
        internal static object Select(object a, object b) { return null; }
        internal static object Where(object a, object b) { return null; }
        internal static object First(object a, int b) { return null; }
        internal static object FirstOrDefault(object a, int b) { return null; }
        internal static object Take(object a, int b) { return null; }
        internal static object Count(object a) { return null; }
        internal static object OrderBy(object a, object b) { return null; }
        internal static object OrderByDescending(object a, object b) { return null; }
        internal static object ThenBy(object a, object b) { return null; }
        internal static object ThenByDescending(object a, object b) { return null; }

        public static object First(ITable a, int b, object c) { return null; }
        public static object FirstOrDefault(ITable a, int b, object c) { return null; }
    }
}
