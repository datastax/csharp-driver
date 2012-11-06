using System;
using System.Collections.Generic;
using System.Text;
using System.Reflection;

namespace Cassandra.Native
{
    internal static partial class TypeInterpreter
    {
        [ThreadStatic]
        static Dictionary<Metadata.ColumnTypeCode, MethodInfo> goMethods = null;

        public static object CqlConvert(byte[] buffer, Metadata.ColumnTypeCode type_code, Metadata.ColumnInfo type_info)
        {
            if (goMethods == null)
                goMethods = new Dictionary<Metadata.ColumnTypeCode, MethodInfo>();
            if (!goMethods.ContainsKey(type_code))
                goMethods.Add(type_code, typeof(TypeInterpreter).GetMethod("ConvertFrom" + (type_code.ToString()), new Type[] { typeof(Metadata.ColumnInfo), typeof(byte[]) }));
            return goMethods[type_code].Invoke(null, new object[] { type_info, buffer });
        }

        [ThreadStatic]
        static Dictionary<Metadata.ColumnTypeCode, MethodInfo> typMethods = null;

        public static Type GetTypeFromCqlType(Metadata.ColumnTypeCode type_code, Metadata.ColumnInfo type_info)
        {
            if (typMethods == null)
                typMethods = new Dictionary<Metadata.ColumnTypeCode, MethodInfo>();
            if (!typMethods.ContainsKey(type_code))
                typMethods.Add(type_code, typeof(TypeInterpreter).GetMethod("GetTypeFrom" + (type_code.ToString()), new Type[] { typeof(Metadata.ColumnInfo) }));
            return (Type)typMethods[type_code].Invoke(null, new object[] { type_info });
        }

        [ThreadStatic]
        static Dictionary<Metadata.ColumnTypeCode, MethodInfo> invMethods = null;

        public static byte[] InvCqlConvert(object value, Metadata.ColumnTypeCode type_code, Metadata.ColumnInfo type_info)
        {
            if (invMethods == null)
                invMethods = new Dictionary<Metadata.ColumnTypeCode, MethodInfo>();
            if (!invMethods.ContainsKey(type_code))
                invMethods.Add(type_code, typeof(TypeInterpreter).GetMethod("InvConvertFrom" + (type_code.ToString()), new Type[] { typeof(Metadata.ColumnInfo), typeof(byte[]) }));
            return (byte[])invMethods[type_code].Invoke(null, new object[] { type_info, value });
        }

        static internal void checkArgument(Type t, object value)
        {
            if (value == null)
                throw new ArgumentNullException();
            else if (!(value.GetType().Equals(t)))
                throw new ArgumentOutOfRangeException("value", value.GetType().FullName, "Should be: " + t.FullName);
        }
        
        static internal void checkArgument<T>(object value)
        {
            if (value == null)
                throw new ArgumentNullException();
            else if (!(value is T))
                throw new ArgumentOutOfRangeException("value", value.GetType().FullName, "Should be: " + typeof(T).FullName);
        }
    }
}
