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

using System.Reflection;

namespace Cassandra.Data.Linq
{
    internal class CqlMthHelps
    {
        internal static MethodInfo SelectMi = typeof (CqlMthHelps).GetTypeInfo().GetMethod("Select", BindingFlags.NonPublic | BindingFlags.Static);
        internal static MethodInfo WhereMi = typeof(CqlMthHelps).GetTypeInfo().GetMethod("Where", BindingFlags.NonPublic | BindingFlags.Static);
        internal static MethodInfo GroupByMi = typeof(CqlMthHelps).GetTypeInfo().GetMethod("GroupBy", BindingFlags.NonPublic | BindingFlags.Static);
        internal static MethodInfo UpdateIfMi = typeof(CqlMthHelps).GetTypeInfo().GetMethod("UpdateIf", BindingFlags.NonPublic | BindingFlags.Static);

        internal static MethodInfo UpdateIfExistsMi = typeof(CqlMthHelps)
            .GetTypeInfo().GetMethod(nameof(UpdateIfExists), BindingFlags.NonPublic | BindingFlags.Static);

        internal static MethodInfo UpdateIfNotExistsMi = typeof(CqlMthHelps)
            .GetTypeInfo().GetMethod(nameof(UpdateIfNotExists), BindingFlags.NonPublic | BindingFlags.Static);
        
        internal static MethodInfo DeleteIfMi = typeof(CqlMthHelps).GetTypeInfo().GetMethod("DeleteIf", BindingFlags.NonPublic | BindingFlags.Static);
        internal static MethodInfo FirstMi = typeof (CqlMthHelps).GetTypeInfo().GetMethod("First", BindingFlags.NonPublic | BindingFlags.Static);

        internal static MethodInfo First_ForCQLTableMi = typeof (CqlMthHelps).GetTypeInfo().GetMethod("First",
                                                                                        new[] {typeof (ITable), typeof (int), typeof (object)});

        internal static MethodInfo FirstOrDefaultMi = typeof (CqlMthHelps).GetTypeInfo().GetMethod("FirstOrDefault", BindingFlags.NonPublic | BindingFlags.Static);

        internal static MethodInfo FirstOrDefault_ForCQLTableMi = typeof (CqlMthHelps).GetTypeInfo().GetMethod("FirstOrDefault",
                                                                                                 new[]
                                                                                                 {typeof (ITable), typeof (int), typeof (object)});

        internal static MethodInfo TakeMi = typeof (CqlMthHelps).GetTypeInfo().GetMethod("Take", BindingFlags.NonPublic | BindingFlags.Static);
        internal static MethodInfo AllowFilteringMi = typeof (CqlMthHelps).GetTypeInfo().GetMethod("AllowFiltering", BindingFlags.NonPublic | BindingFlags.Static);
        internal static MethodInfo CountMi = typeof (CqlMthHelps).GetTypeInfo().GetMethod("Count", BindingFlags.NonPublic | BindingFlags.Static);
        internal static MethodInfo OrderByMi = typeof (CqlMthHelps).GetTypeInfo().GetMethod("OrderBy", BindingFlags.NonPublic | BindingFlags.Static);

        internal static MethodInfo OrderByDescendingMi = typeof (CqlMthHelps).GetTypeInfo().GetMethod("OrderByDescending",
                                                                                        BindingFlags.NonPublic | BindingFlags.Static);

        internal static MethodInfo ThenByMi = typeof (CqlMthHelps).GetTypeInfo().GetMethod("ThenBy", BindingFlags.NonPublic | BindingFlags.Static);

        internal static MethodInfo ThenByDescendingMi = typeof (CqlMthHelps).GetTypeInfo().GetMethod("ThenByDescending",
                                                                                       BindingFlags.NonPublic | BindingFlags.Static);

        internal static object Select(object a, object b)
        {
            return null;
        }

        internal static object Where(object a, object b)
        {
            return null;
        }

        internal static object UpdateIf(object a, object b)
        {
            return null;
        }

        internal static object UpdateIfExists(object a)
        {
            return null;
        }

        internal static object UpdateIfNotExists(object a)
        {
            return null;
        }

        internal static object DeleteIf(object a, object b)
        {
            return null;
        }

        internal static object First(object a, int b)
        {
            return null;
        }

        internal static object FirstOrDefault(object a, int b)
        {
            return null;
        }

        internal static object Take(object a, int b)
        {
            return null;
        }

        internal static object AllowFiltering(object a)
        {
            return null;
        }

        internal static object Count(object a)
        {
            return null;
        }

        internal static object OrderBy(object a, object b)
        {
            return null;
        }

        internal static object GroupBy(object a, object b)
        {
            return null;
        }

        internal static object OrderByDescending(object a, object b)
        {
            return null;
        }

        internal static object ThenBy(object a, object b)
        {
            return null;
        }

        internal static object ThenByDescending(object a, object b)
        {
            return null;
        }

        public static object First(ITable a, int b, object c)
        {
            return null;
        }

        public static object FirstOrDefault(ITable a, int b, object c)
        {
            return null;
        }
    }
}