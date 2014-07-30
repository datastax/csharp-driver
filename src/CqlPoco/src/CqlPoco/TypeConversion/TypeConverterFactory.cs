using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Reflection;

namespace CqlPoco.TypeConversion
{
    /// <summary>
    /// A factory for retrieving Functions capable of converting between two Types.  To use custom Type conversions, inheritors
    /// should derive from this class and implement the GetUserDefinedFromDbConverter method.
    /// </summary>
    public abstract class TypeConverterFactory
    {
        private const BindingFlags PrivateStatic = BindingFlags.NonPublic | BindingFlags.Static;

        private static readonly MethodInfo FindFromDbConverterMethod = typeof (TypeConverterFactory).GetMethod("FindFromDbConverter",
                                                                                                               BindingFlags.NonPublic |
                                                                                                               BindingFlags.Instance);

        private static readonly MethodInfo ConvertToDictionaryMethod = typeof (TypeConverterFactory).GetMethod("ConvertToDictionary", PrivateStatic);

        private static readonly MethodInfo ConvertToHashSetMethod = typeof(TypeConverterFactory).GetMethod("ConvertToHashSet", PrivateStatic);

        private static readonly MethodInfo ConvertToSortedSetMethod = typeof(TypeConverterFactory).GetMethod("ConvertToSortedSet", PrivateStatic);

        private static readonly MethodInfo ConvertToArrayMethod = typeof (Enumerable).GetMethod("ToArray", BindingFlags.Public | BindingFlags.Static);

        private readonly ConcurrentDictionary<Tuple<Type, Type>, Delegate> _converterCache;

        protected TypeConverterFactory()
        {
            _converterCache = new ConcurrentDictionary<Tuple<Type, Type>, Delegate>();
        }

        /// <summary>
        /// Gets a Function that can convert a source type value to a destination type value.
        /// </summary>
        internal Delegate GetFromDbConverter(Type sourceType, Type destinationType)
        {
            return _converterCache.GetOrAdd(Tuple.Create(sourceType, destinationType), _ => GetFromDbConverterImpl(sourceType, destinationType));
        }

        private Delegate GetFromDbConverterImpl(Type sourceType, Type destinationType)
        {
            // Invoke the generic method with our type parameters ()
            return (Delegate) FindFromDbConverterMethod.MakeGenericMethod(sourceType, destinationType).Invoke(this, null);
        }

        /// <summary>
        /// This method is generic because it seems like a good idea to enforce that the abstract method that returns a user-defined Func returns 
        /// one with the correct type parameters, so we'd be invoking that abstract method generically via reflection anyway each time.  So we might
        /// as well make this method generic and invoke it via reflection (it also makes the code for returning the built-in EnumStringMapper func 
        /// simpler since that class is generic).
        /// </summary>
        // ReSharper disable once UnusedMember.Local
        private Delegate FindFromDbConverter<TSource, TDest>()
        {
            // Allow for user-defined conversions
            Delegate converter = GetUserDefinedFromDbConverter<TSource, TDest>();
            if (converter != null)
                return converter;

            Type sourceType = typeof (TSource);
            Type destType = typeof (TDest);

            // Allow strings from the database to be converted to an enum property on a POCO
            if (sourceType == typeof(string) && destType.IsEnum)
            {
                Func<string, TDest> enumMapper = EnumStringMapper<TDest>.MapStringToEnum;
                return enumMapper;
            }

            if (sourceType.IsGenericType && destType.IsGenericType)
            {
                Type sourceGenericDefinition = sourceType.GetGenericTypeDefinition();
                Type[] sourceGenericArgs = sourceType.GetGenericArguments();

                // Allow conversion from IDictionary<,> -> Dictionary<,> since C* driver uses SortedDictionary which can't be cast to Dictionary
                if (sourceGenericDefinition == typeof (IDictionary<,>) && destType == typeof (Dictionary<,>).MakeGenericType(sourceGenericArgs))
                {
                    return ConvertToDictionaryMethod.MakeGenericMethod(sourceGenericArgs).CreateDelegate(typeof (Func<TSource, TDest>));
                }

                // IEnumerable<> could be a Set or a List from Cassandra
                if (sourceGenericDefinition == typeof (IEnumerable<>))
                {
                    // For some reason, the driver uses List<> to represent Sets so allow conversion to HashSet<>, SortedSet<>, and ISet<>
                    if (destType == typeof (HashSet<>).MakeGenericType(sourceGenericArgs))
                    {
                        return ConvertToHashSetMethod.MakeGenericMethod(sourceGenericArgs).CreateDelegate(typeof (Func<TSource, TDest>));
                    }

                    if (destType == typeof (SortedSet<>).MakeGenericType(sourceGenericArgs) ||
                        destType == typeof (ISet<>).MakeGenericType(sourceGenericArgs))
                    {
                        return ConvertToSortedSetMethod.MakeGenericMethod(sourceGenericArgs).CreateDelegate(typeof (Func<TSource, TDest>));
                    }

                    // Allow converting from set/list's IEnumerable<T> to T[]
                    if (destType == sourceGenericArgs[0].MakeArrayType())
                    {
                        return ConvertToArrayMethod.MakeGenericMethod(sourceGenericArgs).CreateDelegate(typeof (Func<TSource, TDest>));
                    }
                }
            }
            
            return null;
        }

        // ReSharper disable UnusedMember.Local 
        // (these methods are invoked via reflection above)
        private static Dictionary<TKey, TValue> ConvertToDictionary<TKey, TValue>(IDictionary<TKey, TValue> mapFromDatabase)
        {
            return new Dictionary<TKey, TValue>(mapFromDatabase);
        }

        private static HashSet<T> ConvertToHashSet<T>(IEnumerable<T> setFromDatabase)
        {
            return new HashSet<T>(setFromDatabase);
        }

        private static SortedSet<T> ConvertToSortedSet<T>(IEnumerable<T> setFromDatabase)
        {
            return new SortedSet<T>(setFromDatabase);
        }
        // ReSharper restore UnusedMember.Local
        
        /// <summary>
        /// Gets any user defined conversion functions that can convert a value of type <see cref="TSource"/> (coming from Cassandra) to a
        /// type of <see cref="TDest"/> (on a POCO).  Return null if no conversion Func is available.
        /// </summary>
        /// <typeparam name="TSource">The Type of the source value from Cassandra to be converted.</typeparam>
        /// <typeparam name="TDest">The Type of the destination value on the POCO.</typeparam>
        /// <returns>A Func that can convert between the two types or null if one is not available.</returns>
        protected abstract Func<TSource, TDest> GetUserDefinedFromDbConverter<TSource, TDest>();
    }
}