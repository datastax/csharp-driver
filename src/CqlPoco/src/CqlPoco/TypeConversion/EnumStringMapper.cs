using System;
using System.Collections.Generic;
using System.Linq;

namespace CqlPoco.TypeConversion
{
    /// <summary>
    /// A class that maps strings to enum values.  Uses a cache internally to speed lookups.
    /// </summary>
    internal static class EnumStringMapper<T>
    {
        private static readonly Dictionary<string, T> StringToEnumCache;

        static EnumStringMapper()
        {
            StringToEnumCache = Enum.GetValues(typeof(T)).Cast<T>().ToDictionary(val => val.ToString());
        }

        public static T MapStringToEnum(string value)
        {
            return StringToEnumCache[value];
        }
    }
}
