using System;
using System.Collections.Generic;

namespace CqlPoco.Utils
{
    /// <summary>
    /// Extension methods to IEnumerable&lt;T&gt;.
    /// </summary>
    internal static class EnumerableExtensions
    {
        public static LookupKeyedCollection<TKey, TValue> ToLookupKeyedCollection<TKey, TValue>(this IEnumerable<TValue> values,
                                                                                        Func<TValue, TKey> keySelector)
        {
            var keyedCollection = new LookupKeyedCollection<TKey, TValue>(keySelector);
            foreach(TValue value in values)
                keyedCollection.Add(value);

            return keyedCollection;
        }

        public static LookupKeyedCollection<TKey, TValue> ToLookupKeyedCollection<TKey, TValue>(this IEnumerable<TValue> values,
                                                                                        Func<TValue, TKey> keySelector,
                                                                                        IEqualityComparer<TKey> keyComparer)
        {
            var keyedCollection = new LookupKeyedCollection<TKey, TValue>(keySelector, keyComparer);
            foreach (TValue value in values)
                keyedCollection.Add(value);

            return keyedCollection;
        }
    }
}
