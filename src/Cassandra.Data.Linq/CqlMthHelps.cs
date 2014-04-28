using System.Reflection;

namespace Cassandra.Data.Linq
{
    internal class CqlMthHelps
    {
        private static CqlMthHelps _instance = new CqlMthHelps();
        internal static MethodInfo SelectMi = typeof (CqlMthHelps).GetMethod("Select", BindingFlags.NonPublic | BindingFlags.Static);
        internal static MethodInfo WhereMi = typeof (CqlMthHelps).GetMethod("Where", BindingFlags.NonPublic | BindingFlags.Static);
        internal static MethodInfo UpdateIfMi = typeof(CqlMthHelps).GetMethod("UpdateIf", BindingFlags.NonPublic | BindingFlags.Static);
        internal static MethodInfo DeleteIfMi = typeof(CqlMthHelps).GetMethod("DeleteIf", BindingFlags.NonPublic | BindingFlags.Static);
        internal static MethodInfo FirstMi = typeof (CqlMthHelps).GetMethod("First", BindingFlags.NonPublic | BindingFlags.Static);

        internal static MethodInfo First_ForCQLTableMi = typeof (CqlMthHelps).GetMethod("First",
                                                                                        new[] {typeof (ITable), typeof (int), typeof (object)});

        internal static MethodInfo FirstOrDefaultMi = typeof (CqlMthHelps).GetMethod("FirstOrDefault", BindingFlags.NonPublic | BindingFlags.Static);

        internal static MethodInfo FirstOrDefault_ForCQLTableMi = typeof (CqlMthHelps).GetMethod("FirstOrDefault",
                                                                                                 new[]
                                                                                                 {typeof (ITable), typeof (int), typeof (object)});

        internal static MethodInfo TakeMi = typeof (CqlMthHelps).GetMethod("Take", BindingFlags.NonPublic | BindingFlags.Static);
        internal static MethodInfo CountMi = typeof (CqlMthHelps).GetMethod("Count", BindingFlags.NonPublic | BindingFlags.Static);
        internal static MethodInfo OrderByMi = typeof (CqlMthHelps).GetMethod("OrderBy", BindingFlags.NonPublic | BindingFlags.Static);

        internal static MethodInfo OrderByDescendingMi = typeof (CqlMthHelps).GetMethod("OrderByDescending",
                                                                                        BindingFlags.NonPublic | BindingFlags.Static);

        internal static MethodInfo ThenByMi = typeof (CqlMthHelps).GetMethod("ThenBy", BindingFlags.NonPublic | BindingFlags.Static);

        internal static MethodInfo ThenByDescendingMi = typeof (CqlMthHelps).GetMethod("ThenByDescending",
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

        internal static object Count(object a)
        {
            return null;
        }

        internal static object OrderBy(object a, object b)
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