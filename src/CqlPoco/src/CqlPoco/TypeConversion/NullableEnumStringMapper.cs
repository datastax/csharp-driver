using System;
using System.Collections.Generic;
using System.Linq;

namespace CqlPoco.TypeConversion
{
    /// <summary>
    /// Static class for mapping between strings and nullable enums.  Uses a cache internally to speed lookups.
    /// </summary>
    internal static class NullableEnumStringMapper<T>
    {
        private static readonly Dictionary<string, T> StringToEnumCache;

        static NullableEnumStringMapper()
        {
            StringToEnumCache = Enum.GetValues(Nullable.GetUnderlyingType(typeof (T))).Cast<T>().ToDictionary(val => val.ToString());
        }

        public static T MapStringToEnum(string value)
        {
            // Account for null strings
            if (value == null)
                return default(T);      // Will return null

            return StringToEnumCache[value];
        }

        public static string MapEnumToString(T enumValue)
        {
            // ReSharper disable once CompareNonConstrainedGenericWithNull (we know this is a Nullable enum from the static constructor)
            return enumValue == null ? null : enumValue.ToString();
        }
    }
}