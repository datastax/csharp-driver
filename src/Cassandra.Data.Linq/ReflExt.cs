//
//      Copyright (C) 2012-2014 DataStax Inc.
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
using System.Reflection;

namespace Cassandra.Data.Linq
{
    internal static class ReflExt
    {
        [ThreadStatic] private static Dictionary<Type, List<MemberInfo>> ReflexionCachePF;

        public static List<MemberInfo> GetPropertiesOrFields(this Type tpy)
        {
            if (ReflexionCachePF == null)
                ReflexionCachePF = new Dictionary<Type, List<MemberInfo>>();

            List<MemberInfo> val;
            if (ReflexionCachePF.TryGetValue(tpy, out val))
                return val;

            var ret = new List<MemberInfo>();
            MemberInfo[] props = tpy.GetMembers();
            foreach (MemberInfo prop in props)
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
            if (prop is FieldInfo)
                return (prop as FieldInfo).GetValue(x);
            throw new InvalidOperationException();
        }

        public static Type GetTypeFromPropertyOrField(this MemberInfo prop)
        {
            if (prop is PropertyInfo)
                return (prop as PropertyInfo).PropertyType;
            if (prop is FieldInfo)
                return (prop as FieldInfo).FieldType;
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