using System;
using System.Collections.Generic;
using System.Text;
using System.Reflection;

namespace Cassandra.Native
{
    internal partial class TypeInterpreter
    {
        static TypeInterpreter()
        {
            RegisterTypeInterpreter(Metadata.ColumnTypeCode.Ascii);            
            RegisterTypeInterpreter(Metadata.ColumnTypeCode.Bigint);
            RegisterTypeInterpreter(Metadata.ColumnTypeCode.Blob);
            RegisterTypeInterpreter(Metadata.ColumnTypeCode.Boolean);
            RegisterTypeInterpreter(Metadata.ColumnTypeCode.Counter);
            RegisterTypeInterpreter(Metadata.ColumnTypeCode.Custom);
            RegisterTypeInterpreter(Metadata.ColumnTypeCode.Decimal);
            RegisterTypeInterpreter(Metadata.ColumnTypeCode.Double);
            RegisterTypeInterpreter(Metadata.ColumnTypeCode.Float);
            RegisterTypeInterpreter(Metadata.ColumnTypeCode.Int);
            RegisterTypeInterpreter(Metadata.ColumnTypeCode.Text);
            RegisterTypeInterpreter(Metadata.ColumnTypeCode.Timestamp);
            RegisterTypeInterpreter(Metadata.ColumnTypeCode.Uuid);
            RegisterTypeInterpreter(Metadata.ColumnTypeCode.Varchar);
            RegisterTypeInterpreter(Metadata.ColumnTypeCode.Varint);
            RegisterTypeInterpreter(Metadata.ColumnTypeCode.Timeuuid);
            RegisterTypeInterpreter(Metadata.ColumnTypeCode.Inet);
            RegisterTypeInterpreter(Metadata.ColumnTypeCode.List);
            RegisterTypeInterpreter(Metadata.ColumnTypeCode.Map);
            RegisterTypeInterpreter(Metadata.ColumnTypeCode.Set);
        }

        delegate object CqlConvertDel(Metadata.ColumnInfo type_info, byte[] buffer);
        delegate Type GetTypeFromCqlTypeDel(Metadata.ColumnInfo type_info);
        delegate byte[] InvCqlConvertDel(Metadata.ColumnInfo type_info, object value);

        static readonly CqlConvertDel[] goMethods = new CqlConvertDel[byte.MaxValue + 1];
        static readonly GetTypeFromCqlTypeDel[] typMethods = new GetTypeFromCqlTypeDel[byte.MaxValue + 1];
        static readonly InvCqlConvertDel[] invMethods = new InvCqlConvertDel[byte.MaxValue + 1];

        internal static void RegisterTypeInterpreter(Metadata.ColumnTypeCode type_code)
        {
            {
                var mth = typeof(TypeInterpreter).GetMethod("ConvertFrom" + (type_code.ToString()), new Type[] { typeof(Metadata.ColumnInfo), typeof(byte[]) });
                goMethods[(byte)type_code] = (CqlConvertDel)Delegate.CreateDelegate(typeof(CqlConvertDel), mth);
            }
            {
                var mth = typeof(TypeInterpreter).GetMethod("GetTypeFrom" + (type_code.ToString()), new Type[] { typeof(Metadata.ColumnInfo) });
                typMethods[(byte)type_code] = (GetTypeFromCqlTypeDel)Delegate.CreateDelegate(typeof(GetTypeFromCqlTypeDel), mth);
            }
            {
                var mth = typeof(TypeInterpreter).GetMethod("InvConvertFrom" + (type_code.ToString()), new Type[] { typeof(Metadata.ColumnInfo), typeof(byte[]) });
                invMethods[(byte)type_code] = (InvCqlConvertDel)Delegate.CreateDelegate(typeof(InvCqlConvertDel), mth);
            }
        }

        public static object CqlConvert(byte[] buffer, Metadata.ColumnTypeCode type_code, Metadata.ColumnInfo type_info)
        {
            return goMethods[(byte)type_code](type_info, buffer);
        }

        public static Type GetTypeFromCqlType(Metadata.ColumnTypeCode type_code, Metadata.ColumnInfo type_info)
        {
            return typMethods[(byte)type_code](type_info);
        }

        public static byte[] InvCqlConvert(object value, Metadata.ColumnTypeCode type_code, Metadata.ColumnInfo type_info)
        {
            return invMethods[(byte)type_code](type_info, value);
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
