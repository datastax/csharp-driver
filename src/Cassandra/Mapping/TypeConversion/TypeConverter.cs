using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using System.Reflection;

namespace Cassandra.Mapping.TypeConversion
{
    /// <summary>
    /// A factory for retrieving Functions capable of converting between two Types.  To use custom Type conversions, inheritors
    /// should derive from this class and implement the <see cref="GetUserDefinedFromDbConverter{TDatabase,TPoco}"/> and
    /// <see cref="GetUserDefinedToDbConverter{TPoco,TDatabase}"/> methods.
    /// </summary>
    public abstract class TypeConverter
    {
        private const BindingFlags PrivateStatic = BindingFlags.NonPublic | BindingFlags.Static;
        private const BindingFlags PrivateInstance = BindingFlags.NonPublic | BindingFlags.Instance;

        private static readonly MethodInfo FindFromDbConverterMethod = typeof (TypeConverter).GetTypeInfo()
            .GetMethod("FindFromDbConverter", PrivateInstance);

        private static readonly MethodInfo FindToDbConverterMethod = typeof (TypeConverter).GetTypeInfo()
            .GetMethod("FindToDbConverter", PrivateInstance);

        private static readonly MethodInfo ConvertToDictionaryMethod = typeof(TypeConverter).GetTypeInfo()
            .GetMethod("ConvertToDictionary", PrivateStatic);

        private static readonly MethodInfo ConvertToDictionaryWithCastMethod = typeof(TypeConverter).GetTypeInfo()
            .GetMethod("ConvertToDictionaryWithCast", PrivateInstance);

        private static readonly MethodInfo ConvertToSortedDictionaryWithCastMethod = typeof(TypeConverter).GetTypeInfo()
            .GetMethod("ConvertToSortedDictionaryWithCast", PrivateInstance);

        private static readonly MethodInfo ConvertToHashSetMethod = typeof(TypeConverter).GetTypeInfo()
            .GetMethod("ConvertToHashSet", PrivateStatic);

        private static readonly MethodInfo ConvertToHashSetWithCastMethod = typeof(TypeConverter).GetTypeInfo()
            .GetMethod("ConvertToHashSetWithCast", PrivateInstance);

        private static readonly MethodInfo ConvertToSortedSetMethod = typeof(TypeConverter).GetTypeInfo()
            .GetMethod("ConvertToSortedSet", PrivateStatic);

        private static readonly MethodInfo ConvertToSortedSetWithCastMethod = typeof(TypeConverter).GetTypeInfo()
            .GetMethod("ConvertToSortedSetWithCast", PrivateInstance);

        private static readonly MethodInfo ConvertToListMethod = typeof(TypeConverter).GetTypeInfo()
            .GetMethod("ConvertToList", PrivateStatic);

        private static readonly MethodInfo ConvertToListWithCastMethod = typeof(TypeConverter).GetTypeInfo()
            .GetMethod("ConvertToListWithCast", PrivateInstance);

        private static readonly MethodInfo ConvertToArrayMethod = typeof(TypeConverter).GetTypeInfo()
            .GetMethod("ConvertToArray", PrivateInstance);

        private readonly ConcurrentDictionary<Tuple<Type, Type>, Delegate> _fromDbConverterCache;
        private readonly ConcurrentDictionary<Tuple<Type, Type>, Delegate> _toDbConverterCache; 

        /// <summary>
        /// Creates a new TypeConverter instance.
        /// </summary>
        protected TypeConverter()
        {
            _fromDbConverterCache = new ConcurrentDictionary<Tuple<Type, Type>, Delegate>();
            _toDbConverterCache = new ConcurrentDictionary<Tuple<Type, Type>, Delegate>();
        }

        /// <summary>
        /// Converts a value of Type <typeparamref name="TValue"/> to a value of Type <typeparamref name="TDatabase"/> using any available converters that would 
        /// normally be used when converting a value for storage in Cassandra.  If no converter is available, wlll throw an InvalidOperationException.
        /// </summary>
        /// <typeparam name="TValue">The value's original Type.</typeparam>
        /// <typeparam name="TDatabase">The Type expected by the database for the parameter.</typeparam>
        /// <param name="value">The value to be converted.</param>
        /// <returns>The converted value.</returns>
        internal TDatabase ConvertCqlArgument<TValue, TDatabase>(TValue value)
        {
            var converter = (Func<TValue, TDatabase>) GetToDbConverter(typeof (TValue), typeof (TDatabase));
            if (converter == null)
            {
                throw new InvalidOperationException(string.Format("No converter is available from Type {0} to Type {1}", typeof(TValue).Name,
                                                                  typeof(TDatabase).Name));
            }

            return converter(value);
        }

        /// <summary>
        /// Gets a Function that can convert a source type value from the database to a destination type value on a POCO.
        /// </summary>
        internal Delegate GetFromDbConverter(Type dbType, Type pocoType)
        {
            return _fromDbConverterCache.GetOrAdd(
                Tuple.Create(dbType, pocoType),
                // Invoke the generic method below with our two type parameters
                _ => (Delegate)FindFromDbConverterMethod.MakeGenericMethod(dbType, pocoType).Invoke(this, null));
        }

        /// <summary>
        /// Gets the conversion function from cache or finds it, throwing if not found.
        /// </summary>
        /// <exception cref="InvalidOperationException" />
        private Func<TSource, TResult> GetFromDbConverter<TSource, TResult>()
        {
            Delegate converter;
            if (typeof(TSource) != typeof(TResult))
            {
                converter = GetFromDbConverter(typeof(TSource), typeof(TResult));
            }
            else
            {
                Func<TSource, TSource> identity = a => a;
                converter = identity;
            }
            if (converter == null)
            {
                throw new InvalidOperationException(
                    string.Format("No converter is available from Type {0} to Type {1}", 
                    typeof(TSource).Name,
                    typeof(TResult).Name));
            }
            return (Func<TSource, TResult>) converter;
        }

        /// <summary>
        /// Gets a Function that can convert a source type value on a POCO to a destination type value for storage in C*.
        /// </summary>
        internal Delegate GetToDbConverter(Type pocoType, Type dbType)
        {
            return _toDbConverterCache.GetOrAdd(Tuple.Create(pocoType, dbType),
                                                _ => (Delegate) FindToDbConverterMethod.MakeGenericMethod(pocoType, dbType).Invoke(this, null));
        }

        /// <summary>
        /// This method is generic because it seems like a good idea to enforce that the abstract method that returns a user-defined Func returns 
        /// one with the correct type parameters, so we'd be invoking that abstract method generically via reflection anyway each time.  So we might
        /// as well make this method generic and invoke it via reflection (it also makes the code for returning the built-in EnumStringMapper func 
        /// simpler since that class is generic).
        /// </summary>
        // ReSharper disable once UnusedMember.Local (invoked via reflection)
        private Delegate FindFromDbConverter<TDatabase, TPoco>()
        {
            // Allow for user-defined conversions
            Delegate converter = GetUserDefinedFromDbConverter<TDatabase, TPoco>();
            if (converter != null)
                return converter;

            Type dbType = typeof (TDatabase);
            Type pocoType = typeof (TPoco);

            // Allow strings from the database to be converted to an enum/nullable enum property on a POCO
            if (dbType == typeof(string))
            {
                if (pocoType.GetTypeInfo().IsEnum)
                {
                    Func<string, TPoco> enumMapper = EnumStringMapper<TPoco>.MapStringToEnum;
                    return enumMapper;
                }

                var underlyingPocoType = Nullable.GetUnderlyingType(pocoType);
                if (underlyingPocoType != null && underlyingPocoType.GetTypeInfo().IsEnum)
                {
                    Func<string, TPoco> enumMapper = NullableEnumStringMapper<TPoco>.MapStringToEnum;
                    return enumMapper;
                }
            }
            if (dbType == typeof(DateTimeOffset))
            {
                if (pocoType == typeof(DateTime))
                {
                    Func<DateTimeOffset, DateTime> dateMapper = d => d.DateTime;
                    return dateMapper;
                }
                if (pocoType == typeof(DateTime?))
                {
                    Func<DateTimeOffset, DateTime?> dateMapper = d => d.DateTime;
                    return dateMapper;
                }
            }
            if (dbType == typeof(Guid) && pocoType == typeof(TimeUuid))
            {
                Func<Guid, TimeUuid> timeUuidMapper = v => (TimeUuid)v;
                return timeUuidMapper;
            }

            if (dbType.GetTypeInfo().IsGenericType)
            {
                Type sourceGenericDefinition = dbType.GetTypeInfo().GetGenericTypeDefinition();
                Type[] sourceGenericArgs = dbType.GetTypeInfo().GetGenericArguments();
                if (pocoType.IsArray && sourceGenericDefinition == typeof(IEnumerable<>))
                {
                    return ConvertToArrayMethod
                        .MakeGenericMethod(sourceGenericArgs[0], pocoType.GetTypeInfo().GetElementType())
                        .CreateDelegateLocal(this);
                }
                if (pocoType.GetTypeInfo().IsGenericType)
                {
                    Type targetGenericType = pocoType.GetTypeInfo().GetGenericTypeDefinition();
                    Type[] targetGenericArgs = pocoType.GetTypeInfo().GetGenericArguments();

                    // Allow conversion from IDictionary<,> -> Dictionary<,> since C* driver uses
                    // SortedDictionary which can't be casted into Dictionary
                    if (typeof(IDictionary<,>).GetTypeInfo().IsAssignableFrom(sourceGenericDefinition))
                    {
                        if (targetGenericType == typeof(Dictionary<,>))
                        {
                            if (sourceGenericArgs[0] == targetGenericArgs[0] && sourceGenericArgs[1] == targetGenericArgs[1])
                            {
                                return ConvertToDictionaryMethod.MakeGenericMethod(sourceGenericArgs).CreateDelegateLocal();
                            }
                            return ConvertToDictionaryWithCastMethod.MakeGenericMethod(
                                sourceGenericArgs[0], sourceGenericArgs[1], targetGenericArgs[0], targetGenericArgs[1])
                                .CreateDelegateLocal(this);
                        }
                        if (targetGenericType == typeof(SortedDictionary<,>))
                        {
                            if (sourceGenericArgs[0] != targetGenericArgs[0] || sourceGenericArgs[1] != targetGenericArgs[1])
                            {
                                return ConvertToSortedDictionaryWithCastMethod.MakeGenericMethod(
                                    sourceGenericArgs[0], sourceGenericArgs[1], targetGenericArgs[0], targetGenericArgs[1])
                                    .CreateDelegateLocal(this);
                            }
                        }
                    }

                    // IEnumerable<> could be a Set or a List from Cassandra
                    if (sourceGenericDefinition == typeof(IEnumerable<>))
                    {
                        if (targetGenericType.GetTypeInfo().IsAssignableFrom(typeof(SortedSet<>)))
                        {
                            if (sourceGenericArgs[0] == targetGenericArgs[0])
                            {
                                return ConvertToSortedSetMethod
                                    .MakeGenericMethod(sourceGenericArgs)
                                    .CreateDelegateLocal();
                            }
                            return ConvertToSortedSetWithCastMethod
                                .MakeGenericMethod(sourceGenericArgs[0], targetGenericArgs[0])
                                .CreateDelegateLocal(this);
                        }
                        if (targetGenericType.GetTypeInfo().IsAssignableFrom(typeof(List<>)))
                        {
                            if (sourceGenericArgs[0] == targetGenericArgs[0])
                            {
                                return ConvertToListMethod
                                    .MakeGenericMethod(sourceGenericArgs)
                                    .CreateDelegateLocal();
                            }
                            return ConvertToListWithCastMethod
                                .MakeGenericMethod(sourceGenericArgs[0], targetGenericArgs[0])
                                .CreateDelegateLocal(this);
                        }
                        if (targetGenericType.GetTypeInfo().IsAssignableFrom(typeof(HashSet<>)))
                        {
                            if (sourceGenericArgs[0] == targetGenericArgs[0])
                            {
                                return ConvertToHashSetMethod
                                    .MakeGenericMethod(sourceGenericArgs)
                                    .CreateDelegateLocal();
                            }
                            return ConvertToHashSetWithCastMethod
                                .MakeGenericMethod(sourceGenericArgs[0], targetGenericArgs[0])
                                .CreateDelegateLocal(this);
                        }
                    }
                }
            }
            
            return null;
        }

        /// <summary>
        /// See note above on why this is generic.
        /// </summary>
        // ReSharper disable once UnusedMember.Local (invoked via reflection)
        private Delegate FindToDbConverter<TPoco, TDatabase>()
        {
            // Allow for user-defined conversions
            Delegate converter = GetUserDefinedToDbConverter<TPoco, TDatabase>();
            if (converter != null)
            {
                return converter;
            }

            Type pocoType = typeof (TPoco);
            Type dbType = typeof (TDatabase);

            // Support enum/nullable enum => string conversion
            if (dbType == typeof (string))
            {
                if (pocoType.GetTypeInfo().IsEnum)
                {
                    // Just call ToStirng() on the enum value from the POCO
                    Func<TPoco, string> enumConverter = prop => prop.ToString();
                    return enumConverter;
                }

                Type underlyingPocoType = Nullable.GetUnderlyingType(pocoType);
                if (underlyingPocoType != null && underlyingPocoType.GetTypeInfo().IsEnum)
                {
                    Func<TPoco, string> enumConverter = NullableEnumStringMapper<TPoco>.MapEnumToString;
                    return enumConverter;
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

        private Dictionary<TKeyResult, TValueResult> ConvertToDictionaryWithCast<TKeySource, TValueSource, TKeyResult, TValueResult>
            (IDictionary<TKeySource, TValueSource> mapFromDatabase)
        {
            var keyConverter = GetFromDbConverter<TKeySource, TKeyResult>();
            var valueConverter = GetFromDbConverter<TValueSource, TValueResult>();
            var dictionary = new Dictionary<TKeyResult, TValueResult>(mapFromDatabase.Count);
            foreach (var kv in mapFromDatabase)
            {
                dictionary.Add(keyConverter(kv.Key), valueConverter(kv.Value));
            }
            return dictionary;
        }

        private SortedDictionary<TKeyResult, TValueResult> ConvertToSortedDictionaryWithCast
            <TKeySource, TValueSource, TKeyResult, TValueResult>
            (IDictionary<TKeySource, TValueSource> mapFromDatabase)
        {
            var keyConverter = GetFromDbConverter<TKeySource, TKeyResult>();
            var valueConverter = GetFromDbConverter<TValueSource, TValueResult>();
            var dictionary = new SortedDictionary<TKeyResult, TValueResult>();
            foreach (var kv in mapFromDatabase)
            {
                dictionary.Add(keyConverter(kv.Key), valueConverter(kv.Value));
            }
            return dictionary;
        }

        private static HashSet<T> ConvertToHashSet<T>(IEnumerable<T> setFromDatabase)
        {
            return new HashSet<T>(setFromDatabase);
        }

        private HashSet<TResult> ConvertToHashSetWithCast<TSource, TResult>(IEnumerable<TSource> setFromDatabase)
        {
            return new HashSet<TResult>(setFromDatabase.Select(GetFromDbConverter<TSource, TResult>()));
        }

        private static SortedSet<T> ConvertToSortedSet<T>(IEnumerable<T> setFromDatabase)
        {
            return new SortedSet<T>(setFromDatabase);
        }

        private SortedSet<TResult> ConvertToSortedSetWithCast<TSource, TResult>(IEnumerable<TSource> setFromDatabase)
        {
            return new SortedSet<TResult>(setFromDatabase.Select(GetFromDbConverter<TSource, TResult>()));
        }

        private static List<T> ConvertToList<T>(IEnumerable<T> itemsDatabase)
        {
            return new List<T>(itemsDatabase);
        }

        private List<TResult> ConvertToListWithCast<TSource, TResult>(IEnumerable<TSource> itemsDatabase)
        {
            return new List<TResult>(itemsDatabase.Select(GetFromDbConverter<TSource, TResult>()));
        }
        
        private TResult[] ConvertToArray<TSource, TResult>(IEnumerable<TSource> listFromDatabase)
        {
            return listFromDatabase.Select(GetFromDbConverter<TSource, TResult>()).ToArray();
        }

        // ReSharper restore UnusedMember.Local

        /// <summary>
        /// Gets any user defined conversion functions that can convert a value of type <typeparamref name="TDatabase"/> (coming from Cassandra) to a
        /// type of <typeparamref name="TPoco"/> (a field or property on a POCO).  Return null if no conversion Func is available.
        /// </summary>
        /// <typeparam name="TDatabase">The Type of the source value from Cassandra to be converted.</typeparam>
        /// <typeparam name="TPoco">The Type of the destination value on the POCO.</typeparam>
        /// <returns>A Func that can convert between the two types or null if one is not available.</returns>
        protected abstract Func<TDatabase, TPoco> GetUserDefinedFromDbConverter<TDatabase, TPoco>();

        /// <summary>
        /// Gets any user defined conversion functions that can convert a value of type <typeparamref name="TPoco"/> (coming from a property/field on a
        /// POCO) to a type of <typeparamref name="TDatabase"/> (the Type expected by Cassandra for the database column).  Return null if no conversion
        /// Func is available.
        /// </summary>
        /// <typeparam name="TPoco">The Type of the source value from the POCO property/field to be converted.</typeparam>
        /// <typeparam name="TDatabase">The Type expected by C* for the database column.</typeparam>
        /// <returns>A Func that can converter between the two Types or null if one is not available.</returns>
        protected abstract Func<TPoco, TDatabase> GetUserDefinedToDbConverter<TPoco, TDatabase>();
    }

    internal static class ReflectionUtils
    {
        public static Delegate CreateDelegateLocal(this MethodInfo method, object sender = null)
        {
            if (method == null)
            {
                throw new ArgumentNullException("method");
            }
            var delegateType = Expression.GetFuncType(
                method.GetParameters().Select(p => p.ParameterType)
                .Concat(new [] { method.ReturnType }).ToArray());
            return method.CreateDelegate(delegateType, sender);
        }
    }
}