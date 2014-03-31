using System;
using System.Collections.Generic;
using System.Reflection;

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
}