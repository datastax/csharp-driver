using System;
using System.Collections.Generic;
using System.Linq;

namespace CqlPoco.TypeConversion
{
    /// <summary>
    /// Static class for mapping between strings and nullable enums.  Uses a cache internally to speed lookups.
    /// </summary>
    public static class NullableEnumStringMapper<T>
    {
        private static readonly Dictionary<string, T> StringToEnumCache;

        static NullableEnumStringMapper()
        {
            StringToEnumCache = Enum.GetValues(Nullable.GetUnderlyingType(typeof (T))).Cast<T>().ToDictionary(val => val.ToString());
        }

        /// <summary>
        /// Converts a string value to a nullable enum value of Type T.
        /// </summary>
        public static T MapStringToEnum(string value)
        {
            // Account for null strings
            if (value == null)
                return default(T);      // Will return null

            return StringToEnumCache[value];
        }

        /// <summary>
        /// Converts a nullable enum value of Type T to a string.
        /// </summary>
        public static string MapEnumToString(T enumValue)
        {
            // ReSharper disable once CompareNonConstrainedGenericWithNull (we know this is a Nullable enum from the static constructor)
            return enumValue == null ? null : enumValue.ToString();
        }
    }
}