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
            RegisterTypeInterpreter(TableMetadata.ColumnTypeCode.Ascii);            
            RegisterTypeInterpreter(TableMetadata.ColumnTypeCode.Bigint);
            RegisterTypeInterpreter(TableMetadata.ColumnTypeCode.Blob);
            RegisterTypeInterpreter(TableMetadata.ColumnTypeCode.Boolean);
            RegisterTypeInterpreter(TableMetadata.ColumnTypeCode.Counter);
            RegisterTypeInterpreter(TableMetadata.ColumnTypeCode.Custom);
            RegisterTypeInterpreter(TableMetadata.ColumnTypeCode.Double);
            RegisterTypeInterpreter(TableMetadata.ColumnTypeCode.Float);
            RegisterTypeInterpreter(TableMetadata.ColumnTypeCode.Int);
            RegisterTypeInterpreter(TableMetadata.ColumnTypeCode.Text);
            RegisterTypeInterpreter(TableMetadata.ColumnTypeCode.Timestamp);
            RegisterTypeInterpreter(TableMetadata.ColumnTypeCode.Uuid);
            RegisterTypeInterpreter(TableMetadata.ColumnTypeCode.Varchar);
            RegisterTypeInterpreter(TableMetadata.ColumnTypeCode.Timeuuid);
            RegisterTypeInterpreter(TableMetadata.ColumnTypeCode.Inet);
            RegisterTypeInterpreter(TableMetadata.ColumnTypeCode.List);
            RegisterTypeInterpreter(TableMetadata.ColumnTypeCode.Map);
            RegisterTypeInterpreter(TableMetadata.ColumnTypeCode.Set);
#if NET_40_OR_GREATER
            RegisterTypeInterpreter(TableMetadata.ColumnTypeCode.Decimal);
            RegisterTypeInterpreter(TableMetadata.ColumnTypeCode.Varint);
#endif
        }

        delegate object CqlConvertDel(TableMetadata.ColumnInfo type_info, byte[] buffer);
        delegate Type GetTypeFromCqlTypeDel(TableMetadata.ColumnInfo type_info);
        delegate byte[] InvCqlConvertDel(TableMetadata.ColumnInfo type_info, object value);

        static readonly CqlConvertDel[] goMethods = new CqlConvertDel[byte.MaxValue + 1];
        static readonly GetTypeFromCqlTypeDel[] typMethods = new GetTypeFromCqlTypeDel[byte.MaxValue + 1];
        static readonly InvCqlConvertDel[] invMethods = new InvCqlConvertDel[byte.MaxValue + 1];

        internal static void RegisterTypeInterpreter(TableMetadata.ColumnTypeCode type_code)
        {
            {
                var mth = typeof(TypeInterpreter).GetMethod("ConvertFrom" + (type_code.ToString()), new Type[] { typeof(TableMetadata.ColumnInfo), typeof(byte[]) });
                goMethods[(byte)type_code] = (CqlConvertDel)Delegate.CreateDelegate(typeof(CqlConvertDel), mth);
            }
            {
                var mth = typeof(TypeInterpreter).GetMethod("GetTypeFrom" + (type_code.ToString()), new Type[] { typeof(TableMetadata.ColumnInfo) });
                typMethods[(byte)type_code] = (GetTypeFromCqlTypeDel)Delegate.CreateDelegate(typeof(GetTypeFromCqlTypeDel), mth);
            }
            {
                var mth = typeof(TypeInterpreter).GetMethod("InvConvertFrom" + (type_code.ToString()), new Type[] { typeof(TableMetadata.ColumnInfo), typeof(byte[]) });
                invMethods[(byte)type_code] = (InvCqlConvertDel)Delegate.CreateDelegate(typeof(InvCqlConvertDel), mth);
            }
        }

        public static object CqlConvert(byte[] buffer, TableMetadata.ColumnTypeCode type_code, TableMetadata.ColumnInfo type_info)
        {
            return goMethods[(byte)type_code](type_info, buffer);
        }

        public static Type GetTypeFromCqlType(TableMetadata.ColumnTypeCode type_code, TableMetadata.ColumnInfo type_info)
        {
            return typMethods[(byte)type_code](type_info);
        }

        public static byte[] InvCqlConvert(object value, TableMetadata.ColumnTypeCode type_code, TableMetadata.ColumnInfo type_info)
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
