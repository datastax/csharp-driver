using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;

namespace CqlPoco.IntegrationTests.TestData
{
    /// <summary>
    /// Static utility class to help with generating test data.
    /// </summary>
    public static class TestDataGenerator
    {
        private static readonly ConcurrentDictionary<Type, object[]> EnumValuesCache = new ConcurrentDictionary<Type, object[]>();

        public static TEnum GetEnumValue<TEnum>(int index)
        {
            bool isNullableEnum = IsNullableType(typeof(TEnum));

            // Get the enum type, taking into account nullable enums
            Type enumType = isNullableEnum ? Nullable.GetUnderlyingType(typeof(TEnum)) : typeof(TEnum);

            // Get the available enum values
            object[] enumValues = EnumValuesCache.GetOrAdd(enumType, t => Enum.GetValues(enumType).Cast<object>().ToArray());

            // If not a nullable enum, use index with mod to pick an available value
            if (isNullableEnum == false)
                return (TEnum) enumValues[index % enumValues.Length];

            // If a nullable enum, we want to generate null also so treat an index of length + 1 as null
            int idx = index % (enumValues.Length + 1);
            if (idx < enumValues.Length)
                return (TEnum) enumValues[idx];

            return default(TEnum);
        }

        public static DateTimeOffset GetDateTimeInPast(int index)
        {
            return DateTimeOffset.UtcNow.AddDays(-1 * index);
        }

        public static DateTimeOffset? GetNullableDateTimeInPast(int index)
        {
            // Just null out every third record
            if (index % 3 == 0)
                return null;

            return GetDateTimeInPast(index);
        }

        public static List<T> GetList<T>(int index, Func<int, T> factory)
        {
            int elementsInList = index % 5;
            if (elementsInList == 0)
                return new List<T>();

            return Enumerable.Range(0, elementsInList).Select(factory).ToList();
        }

        public static HashSet<T> GetSet<T>(int index, Func<int, T> factory)
        {
            int elementsInSet = index % 3;
            if (elementsInSet == 0)
                return new HashSet<T>();

            return new HashSet<T>(Enumerable.Range(0, elementsInSet).Select(factory));
        }

        public static Dictionary<TKey, TValue> GetDictionary<TKey, TValue>(int index, Func<int, TKey> keyFactory, Func<int, TValue> valueFactory)
        {
            int elementsInDictionary = index % 4;
            if (elementsInDictionary == 0)
                return new Dictionary<TKey, TValue>();

            return Enumerable.Range(0, elementsInDictionary).ToDictionary(keyFactory, valueFactory);
        }

        public static bool IsNullableType(Type t)
        {
            return t.IsGenericType && t.GetGenericTypeDefinition() == typeof(Nullable<>);
        }
    }
}
