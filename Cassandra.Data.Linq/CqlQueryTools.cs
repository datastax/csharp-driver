using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Text.RegularExpressions;
using System.Globalization;
using System.Reflection;
using Cassandra.Native;

namespace Cassandra.Data
{
    internal static class ReflExt
    {
        public static IEnumerable<MemberInfo> GetPropertiesOrFields(this Type tpy)
        {
            var props = tpy.GetMembers();
            foreach (var prop in props)
            {
                if (prop is PropertyInfo || prop is FieldInfo)
                    yield return prop;
            }
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
        static Regex IdentifierRx = new Regex(@"(?<low>[a-z][a-z0-9_]*)?(?<hi>[a-zA-Z][a-zA-Z0-9_]*)?", RegexOptions.Compiled);

        public static string CqlIdentifier(this string id)
        {
            if (!string.IsNullOrWhiteSpace(id))
            {
                var m = IdentifierRx.Match(id);
                if (m.Success)
                {
                    if (m.Groups["hi"].Length > 0)
                    {
                        return "\"" + id + "\"";
                    }
                    else
                    {
                        return id;
                    }
                }
            }
            throw new ArgumentException("invalid identifier");
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
            StringBuilder stringBuilder = new StringBuilder();
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
            if (obj is String) return Encode(obj as String);
            else if (obj is Boolean) return Encode((Boolean)obj);
            else if (obj is byte[]) return Encode((byte[])obj);
            else if (obj is Double) return Encode((Double)obj);
            else if (obj is Single) return Encode((Single)obj);
            else if (obj is Decimal) return Encode((Decimal)obj);
            else if (obj is DateTimeOffset) return Encode((DateTimeOffset)obj);
            else return obj.ToString();
        }

        public static string Encode(string str)
        {
            return '\'' + str.Replace("\'", "\'\'") + '\'';
        }

        public static string Encode(bool val)
        {
            return '\'' + (val ? "true" : "false") + '\'';
        }

        public static string Encode(byte[] val)
        {
            return "\'" + val.ToHex() + "\'";
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
            return ToUnixTime(val).ToString();
        }

        static readonly DateTimeOffset UnixStart = new DateTimeOffset(1970, 1, 1, 0, 0, 0, TimeSpan.Zero);

        public static long ToUnixTime(DateTimeOffset dt)
        {
            if (dt == DateTimeOffset.MinValue) return 0;
            // this was changed from .NET Ticks to the Unix Epoch to be compatible with other cassandra libraries
            return Convert.ToInt64(Math.Floor((dt - UnixStart).TotalMilliseconds));
        }

        static readonly Dictionary<Type, string> CQLTypeNames = new Dictionary<Type, string>() {
        { typeof(Int32), "int" }, 
        { typeof(Int64), "bigint" }, 
        { typeof(String), "text" }, 
        { typeof(byte[]), "blob" },
        { typeof(Boolean), "boolean" },
        { typeof(Decimal), "decimal" },
        { typeof(Double), "double" },
        { typeof(Single), "float" },
        { typeof(Guid), "uuid" },
        { typeof(Nullable<Guid>), "uuid" },
        { typeof(DateTimeOffset), "timestamp" },
        };

        public static string GetCreateKeyspaceCQL(string keyspace)
        {
            return string.Format(
  @"CREATE KEYSPACE {0} 
  WITH replication = {{ 'class' : 'SimpleStrategy', 'replication_factor' : 2 }}"
              , keyspace.CqlIdentifier());
        }

        public static string GetUseKeyspaceCQL(string keyspace)
        {
            return string.Format(
  @"USE {0}"
              , keyspace.CqlIdentifier());
        }

        public static string GetDropKeyspaceCQL(string keyspace)
        {
            return string.Format(
  @"DROP KEYSPACE {0}"
              , keyspace.CqlIdentifier());
        }

        public static string GetCreateCQL(ICqlTable table, string tablename = null)
        {
            StringBuilder ret = new StringBuilder();
            ret.Append("CREATE TABLE ");
            ret.Append((tablename ?? table.GetEntityType().Name).CqlIdentifier());
            ret.Append("(");

            SortedDictionary<int, string> keys = new SortedDictionary<int, string>();
            string partitionKey = null;
            var props = table.GetEntityType().GetPropertiesOrFields();
            int curLevel = 0;
            foreach (var prop in props)
            {
                Type tpy = prop.GetTypeFromPropertyOrField();
                ret.Append(prop.Name.CqlIdentifier());
                ret.Append(" ");
                ret.Append(CQLTypeNames[tpy]);
                ret.Append(",");
                var pk = prop.GetCustomAttributes(typeof(PartitionKeyAttribute), true).FirstOrDefault() as PartitionKeyAttribute;
                if (pk != null)
                {
                    if (partitionKey != null)
                        throw new ArgumentException();
                    partitionKey = prop.Name;
                }
                else
                {
                    var rk = prop.GetCustomAttributes(typeof(RowKeyAttribute), true).FirstOrDefault() as RowKeyAttribute;
                    if (rk != null)
                    {
                        var idx = rk.Index;
                        if (idx == -1)
                            idx = curLevel++;
                        keys.Add(idx, prop.Name);
                    }
                }
            }
            ret.Append("PRIMARY KEY(");
            ret.Append(partitionKey.CqlIdentifier());
            foreach (var kv in keys)
            {
                ret.Append(",");
                ret.Append(kv.Value.CqlIdentifier());
            }
            ret.Append("));");
            return ret.ToString();
        }

        public static string GetInsertCQL(object row, string tablename = null)
        {
            var rowType = row.GetType();
            StringBuilder ret = new StringBuilder();
            ret.Append("INSERT INTO ");
            ret.Append((tablename ?? rowType.Name).CqlIdentifier());
            ret.Append("(");

            var props = rowType.GetPropertiesOrFields();
            bool first = true;
            foreach (var prop in props)
            {
                if (first) first = false; else ret.Append(",");
                ret.Append(prop.Name.CqlIdentifier());
            }
            ret.Append(") VALUES (");
            first = true;
            foreach (var prop in props)
            {
                if (first) first = false; else ret.Append(",");
                ret.Append(prop.GetValueFromPropertyOrField(row).Encode());
            }
            ret.Append(");");
            return ret.ToString();
        }

        public static string GetUpdateCQL(object row, object newRow, string tablename = null, bool all = false)
        {
            var rowType = row.GetType();
            StringBuilder SET = new StringBuilder();
            StringBuilder WHERE = new StringBuilder();
            var props = rowType.GetPropertiesOrFields();
            bool firstSet = true;
            bool firstWhere = true;
            bool changeDetected = false;
            foreach (var prop in props)
            {
                var pk = prop.GetCustomAttributes(typeof(PartitionKeyAttribute), true).FirstOrDefault() as PartitionKeyAttribute;
                if (pk == null)
                {
                    var rk = prop.GetCustomAttributes(typeof(RowKeyAttribute), true).FirstOrDefault() as RowKeyAttribute;
                    if (rk == null)
                    {
                        var newVal = prop.GetValueFromPropertyOrField(newRow);
                        bool areDifferent = !prop.GetValueFromPropertyOrField(row).Equals(newVal);
                        if (all || (areDifferent))
                        {
                            if (areDifferent)
                                changeDetected = true;
                            if (firstSet) firstSet = false; else SET.Append(",");
                            SET.Append(prop.Name.CqlIdentifier());
                            SET.Append("=");
                            SET.Append(Encode(newVal));
                        }
                        continue;
                    }
                }
                var pv = prop.GetValueFromPropertyOrField(row);
                if (pv != null)
                {
                    if (firstWhere) firstWhere = false; else WHERE.Append(" AND ");
                    WHERE.Append(prop.Name.CqlIdentifier());
                    WHERE.Append("=");
                    WHERE.Append(Encode(pv));
                }
            }

            if (!changeDetected)
                return null;

            StringBuilder ret = new StringBuilder();
            ret.Append("UPDATE ");
            ret.Append((tablename ?? rowType.Name).CqlIdentifier());
            ret.Append(" SET ");
            ret.Append(SET);
            ret.Append(" WHERE ");
            ret.Append(WHERE);
            ret.Append(";");
            return ret.ToString();
        }
        
        public static string GetDeleteCQL(object row, string tablename = null)
        {
            var rowType = row.GetType();

            StringBuilder ret = new StringBuilder();
            ret.Append("DELETE FROM ");
            ret.Append((tablename ?? rowType.Name).CqlIdentifier());
            ret.Append(" WHERE ");

            var props = rowType.GetPropertiesOrFields();
            bool first = true;
            foreach (var prop in props)
            {
                var pk = prop.GetCustomAttributes(typeof(PartitionKeyAttribute), true).FirstOrDefault() as PartitionKeyAttribute;
                if (pk == null)
                {
                    var rk = prop.GetCustomAttributes(typeof(RowKeyAttribute), true).FirstOrDefault() as RowKeyAttribute;
                    if (rk == null)
                    {
                        continue;
                    }
                }
                var pv = prop.GetValueFromPropertyOrField(row);
                if (pv != null)
                {
                    if (first) first = false; else ret.Append(" AND ");
                    ret.Append(prop.Name.CqlIdentifier());
                    ret.Append("=");
                    ret.Append(Encode(pv));
                }
            }
            ret.Append(";");
            return ret.ToString();
        }

        public static T GetRowFromCqlRow<T>(CqlRow cqlRow, Dictionary<string, int> colToIdx, Dictionary<string, string> alter)
        {
            var ncstr = typeof(T).GetConstructor(new Type[] { });
            if (ncstr != null)
            {
                T row = (T)ncstr.Invoke(new object[] { });

                var props = typeof(T).GetMembers();
                foreach (var prop in props)
                {
                    if (prop is PropertyInfo)
                    {
                        int idx;
                        if (colToIdx.ContainsKey(prop.Name))
                            idx = colToIdx[prop.Name];
                        else
                            idx = colToIdx[alter[prop.Name]];
                        var val = cqlRow[idx];
                        (prop as PropertyInfo).SetValue(row, val, null);
                    }
                    else if (prop is FieldInfo)
                    {
                        int idx;
                        if (colToIdx.ContainsKey(prop.Name))
                            idx = colToIdx[prop.Name];
                        else
                            idx = colToIdx[alter[prop.Name]];
                        var val = cqlRow[idx];
                        (prop as FieldInfo).SetValue(row, val);
                    }
                }
                return row;
            }
            else
            {
                if (cqlRow.Length == 1 && (typeof(T).IsPrimitive || typeof(T) == typeof(Decimal) || typeof(T) == typeof(String)))
                {
                    return (T) cqlRow[0];
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
                        object[] objs = new object[cqlRow.Length];
                        var props = typeof(T).GetMembers();
                        int idx = 0;
                        foreach (var prop in props)
                        {
                            if (prop is PropertyInfo || prop is FieldInfo)
                            {
                                var val = cqlRow[idx];
                                objs[idx] = val;
                                idx++;
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
        static CqlMthHelps Instance = new CqlMthHelps();
        internal static MethodInfo SelectMi = typeof(CqlMthHelps).GetMethod("Select", BindingFlags.NonPublic | BindingFlags.Static);
        internal static MethodInfo WhereMi = typeof(CqlMthHelps).GetMethod("Where", BindingFlags.NonPublic | BindingFlags.Static);
        internal static MethodInfo TakeMi = typeof(CqlMthHelps).GetMethod("Take", BindingFlags.NonPublic | BindingFlags.Static);
        internal static MethodInfo CountMi = typeof(CqlMthHelps).GetMethod("Count", BindingFlags.NonPublic | BindingFlags.Static);
        internal static MethodInfo OrderByMi = typeof(CqlMthHelps).GetMethod("OrderBy", BindingFlags.NonPublic | BindingFlags.Static);
        internal static MethodInfo OrderByDescendingMi = typeof(CqlMthHelps).GetMethod("OrderByDescending", BindingFlags.NonPublic | BindingFlags.Static);
        internal static MethodInfo ThenByMi = typeof(CqlMthHelps).GetMethod("ThenBy", BindingFlags.NonPublic | BindingFlags.Static);
        internal static MethodInfo ThenByDescendingMi = typeof(CqlMthHelps).GetMethod("ThenByDescending", BindingFlags.NonPublic | BindingFlags.Static);
        internal static object Select(object a, object b) { return null; }
        internal static object Where(object a, object b) { return null; }
        internal static object Take(object a, int b) { return null; }
        internal static object Count(object a) { return null; }
        internal static object OrderBy(object a, object b) { return null; }
        internal static object OrderByDescending(object a, object b) { return null; }
        internal static object ThenBy(object a, object b) { return null; }
        internal static object ThenByDescending(object a, object b) { return null; }
    }
}
