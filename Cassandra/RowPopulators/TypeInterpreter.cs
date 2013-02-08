using System;

namespace Cassandra
{
    internal partial class TypeInterpreter
    {
        static TypeInterpreter()
        {
            RegisterTypeInterpreter(ColumnTypeCode.Ascii);            
            RegisterTypeInterpreter(ColumnTypeCode.Bigint);
            RegisterTypeInterpreter(ColumnTypeCode.Blob);
            RegisterTypeInterpreter(ColumnTypeCode.Boolean);
            RegisterTypeInterpreter(ColumnTypeCode.Counter);
            RegisterTypeInterpreter(ColumnTypeCode.Custom);
            RegisterTypeInterpreter(ColumnTypeCode.Double);
            RegisterTypeInterpreter(ColumnTypeCode.Float);
            RegisterTypeInterpreter(ColumnTypeCode.Int);
            RegisterTypeInterpreter(ColumnTypeCode.Text);
            RegisterTypeInterpreter(ColumnTypeCode.Timestamp);
            RegisterTypeInterpreter(ColumnTypeCode.Uuid);
            RegisterTypeInterpreter(ColumnTypeCode.Varchar);
            RegisterTypeInterpreter(ColumnTypeCode.Timeuuid);
            RegisterTypeInterpreter(ColumnTypeCode.Inet);
            RegisterTypeInterpreter(ColumnTypeCode.List);
            RegisterTypeInterpreter(ColumnTypeCode.Map);
            RegisterTypeInterpreter(ColumnTypeCode.Set);
#if NET_40_OR_GREATER
            RegisterTypeInterpreter(ColumnTypeCode.Decimal);
            RegisterTypeInterpreter(ColumnTypeCode.Varint);
#endif
        }

        delegate object CqlConvertDel(IColumnInfo type_info, byte[] buffer);
        delegate Type GetTypeFromCqlTypeDel(IColumnInfo type_info);
        delegate byte[] InvCqlConvertDel(IColumnInfo type_info, object value);

        static readonly CqlConvertDel[] GoMethods = new CqlConvertDel[byte.MaxValue + 1];
        static readonly GetTypeFromCqlTypeDel[] TypMethods = new GetTypeFromCqlTypeDel[byte.MaxValue + 1];
        static readonly InvCqlConvertDel[] InvMethods = new InvCqlConvertDel[byte.MaxValue + 1];

        internal static void RegisterTypeInterpreter(ColumnTypeCode type_code)
        {
            {
                var mth = typeof(TypeInterpreter).GetMethod("ConvertFrom" + (type_code.ToString()), new Type[] { typeof(IColumnInfo), typeof(byte[]) });
                GoMethods[(byte)type_code] = (CqlConvertDel)Delegate.CreateDelegate(typeof(CqlConvertDel), mth);
            }
            {
                var mth = typeof(TypeInterpreter).GetMethod("GetTypeFrom" + (type_code.ToString()), new Type[] { typeof(IColumnInfo) });
                TypMethods[(byte)type_code] = (GetTypeFromCqlTypeDel)Delegate.CreateDelegate(typeof(GetTypeFromCqlTypeDel), mth);
            }
            {
                var mth = typeof(TypeInterpreter).GetMethod("InvConvertFrom" + (type_code.ToString()), new Type[] { typeof(IColumnInfo), typeof(byte[]) });
                InvMethods[(byte)type_code] = (InvCqlConvertDel)Delegate.CreateDelegate(typeof(InvCqlConvertDel), mth);
            }
        }

        public static object CqlConvert(byte[] buffer, ColumnTypeCode type_code, IColumnInfo type_info)
        {
            return GoMethods[(byte)type_code](type_info, buffer);
        }

        public static Type GetTypeFromCqlType(ColumnTypeCode type_code, IColumnInfo type_info)
        {
            return TypMethods[(byte)type_code](type_info);
        }

        public static byte[] InvCqlConvert(object value, ColumnTypeCode type_code, IColumnInfo type_info)
        {
            return InvMethods[(byte)type_code](type_info, value);
        }

        static internal void CheckArgument(Type t, object value)
        {
            if (value == null)
                throw new ArgumentNullException();
            else if (!(value.GetType().Equals(t)))
                throw new ArgumentOutOfRangeException("value", value.GetType().FullName, "Should be: " + t.FullName);
        }
        
        static internal void CheckArgument<T>(object value)
        {
            if (value == null)
                throw new ArgumentNullException();
            else if (!(value is T))
                throw new ArgumentOutOfRangeException("value", value.GetType().FullName, "Should be: " + typeof(T).FullName);
        }

        static internal void CheckArgument<T1,T2>(object value)
        {
            if (value == null)
                throw new ArgumentNullException();
            else if ( !(value is T1 || value is T2) )
                throw new ArgumentOutOfRangeException("value", value.GetType().FullName, "Should be: " + typeof(T1).FullName + " or " + typeof(T2).FullName);
        }
    }
}
