using System;
using System.Collections;
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
            .GetMethod(nameof(FindFromDbConverter), PrivateInstance);

        private static readonly MethodInfo FindToDbConverterMethod = typeof (TypeConverter).GetTypeInfo()
            .GetMethod(nameof(FindToDbConverter), PrivateInstance);

        private static readonly MethodInfo ConvertToDictionaryMethod = typeof(TypeConverter).GetTypeInfo()
            .GetMethod(nameof(ConvertToDictionary), PrivateStatic);

        private static readonly MethodInfo ConvertToDictionaryFromDbMethod = typeof(TypeConverter).GetTypeInfo()
            .GetMethod(nameof(ConvertToDictionaryFromDb), PrivateInstance);

        private static readonly MethodInfo ConvertToSortedDictionaryFromDbMethod = typeof(TypeConverter).GetTypeInfo()
            .GetMethod(nameof(ConvertToSortedDictionaryFromDb), PrivateInstance);

        private static readonly MethodInfo ConvertToHashSetMethod = typeof(TypeConverter).GetTypeInfo()
            .GetMethod(nameof(ConvertToHashSet), PrivateStatic);

        private static readonly MethodInfo ConvertToHashSetFromDbMethod = typeof(TypeConverter).GetTypeInfo()
            .GetMethod(nameof(ConvertToHashSetFromDb), PrivateInstance);

        private static readonly MethodInfo ConvertToSortedSetMethod = typeof(TypeConverter).GetTypeInfo()
            .GetMethod(nameof(ConvertToSortedSet), PrivateStatic);

        private static readonly MethodInfo ConvertToSortedSetFromDbMethod = typeof(TypeConverter).GetTypeInfo()
            .GetMethod(nameof(ConvertToSortedSetFromDb), PrivateInstance);

        private static readonly MethodInfo ConvertToListMethod = typeof(TypeConverter).GetTypeInfo()
            .GetMethod(nameof(ConvertToList), PrivateStatic);

        private static readonly MethodInfo ConvertToListFromDbMethod = typeof(TypeConverter).GetTypeInfo()
            .GetMethod(nameof(ConvertToListFromDb), PrivateInstance);

        private static readonly MethodInfo ConvertToArrayFromDbMethod = typeof(TypeConverter).GetTypeInfo()
            .GetMethod(nameof(ConvertToArrayFromDb), PrivateInstance);

        private static readonly MethodInfo ConvertIEnumerableToDbTypeMethod = typeof(TypeConverter)
            .GetTypeInfo().GetMethod(nameof(ConvertIEnumerableToDbType), PrivateInstance);

        private static readonly MethodInfo ConvertIDictionaryToDbTypeMethod = typeof(TypeConverter)
            .GetTypeInfo().GetMethod(nameof(ConvertIDictionaryToDbType), PrivateInstance);

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
                throw new InvalidOperationException($"No converter is available from Type {typeof(TValue).Name} to Type {typeof(TDatabase).Name}");
            }

            return converter(value);
        }

        /// <summary>
        /// Converts a UDT field value (POCO) to to a destination type value for storage in C*.
        /// </summary>
        internal object ConvertToDbFromUdtFieldValue(Type valueType, Type dbType, object value)
        {
            if (valueType.GetTypeInfo().IsGenericType && valueType.GetGenericTypeDefinition() == typeof(Nullable<>))
            {
                var nullableType = typeof(Nullable<>).MakeGenericType(dbType);
                if (valueType == nullableType)
                {
                    return value;
                }
            }

            var converter = GetToDbConverter(valueType, dbType);
            if (converter == null)
            {
                throw new InvalidTypeException($"No converter is available from Type {valueType} is not convertible to type {dbType}");
            }

            return converter.DynamicInvoke(value);
        }

        /// <summary>
        /// Converts a source type value from the database to a destination type value on a POCO.
        /// </summary>
        internal object ConvertToUdtFieldFromDbValue(Type dbType, Type valueType, object value)
        {
            var converter = TryGetFromDbConverter(dbType, valueType);
            if (converter == null)
            {
                throw new InvalidTypeException($"No converter is available from Type {dbType} is not convertible to type {valueType}");
            }

            return converter.DynamicInvoke(value);
        }

        /// <summary>
        /// Gets a Function that can convert a source type value from the database to a destination type value on a POCO.
        /// </summary>
        internal Delegate TryGetFromDbConverter(Type dbType, Type pocoType)
        {
            return _fromDbConverterCache.GetOrAdd(
                Tuple.Create(dbType, pocoType),
                // Invoke the generic method below with our two type parameters
                _ => (Delegate)FindFromDbConverterMethod.MakeGenericMethod(dbType, pocoType).Invoke(this, null));
        }

        /// <summary>
        /// Gets the conversion function from cache or tries to cast.
        /// </summary>
        private Func<TSource, TResult> TryGetFromDbConverter<TSource, TResult>()
        {
            Delegate converter;
            if (typeof(TSource) != typeof(TResult))
            {
                converter = TryGetFromDbConverter(typeof(TSource), typeof(TResult));
                if (converter == null)
                {
                    // Try cast
                    TResult ChangeType(TSource a)
                    {
                        try
                        {
                            return (TResult) (object) a;
                        }
                        catch (InvalidCastException ex)
                        {
                            throw new InvalidCastException(
                                $"Specified cast is not valid: from {a.GetType()} to {typeof(TResult)}", ex);
                        }
                    }

                    return ChangeType;
                }
            }
            else
            {
                Func<TSource, TSource> identity = a => a;
                converter = identity;
            }
            if (converter == null)
            {
                throw new InvalidOperationException(
                    $"No converter is available from Type {typeof(TSource).Name} to Type {typeof(TResult).Name}");
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
        /// This method is generic because it seems like a good idea to enforce that the abstract method that returns
        /// a user-defined Func returns one with the correct type parameters, so we'd be invoking that abstract method
        /// generically via reflection anyway each time.  So we might as well make this method generic and invoke it
        /// via reflection (it also makes the code for returning the built-in EnumStringMapper func simpler since that
        /// class is generic).
        /// </summary>
        /// <returns>A delegate or null.</returns>
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
                    return ConvertToArrayFromDbMethod
                        .MakeGenericMethod(sourceGenericArgs[0], pocoType.GetTypeInfo().GetElementType())
                        .CreateDelegateLocal(this);
                }
                if (pocoType.GetTypeInfo().IsGenericType)
                {
                    var targetGenericType = pocoType.GetTypeInfo().GetGenericTypeDefinition();
                    var targetGenericArgs = pocoType.GetTypeInfo().GetGenericArguments();
                    
                    if (sourceGenericDefinition == typeof(IDictionary<,>))
                    {
                        return ConvertFromIDictionary(targetGenericType, sourceGenericArgs, targetGenericArgs,
                            pocoType);
                    }

                    // IEnumerable<> could be a Set or a List from Cassandra
                    if (sourceGenericDefinition == typeof(IEnumerable<>))
                    {
                        return ConvertFromIEnumerable(targetGenericType, sourceGenericArgs, targetGenericArgs,
                            pocoType);
                    }
                }
            }
            
            return null;
        }

        private Delegate ConvertFromIDictionary(Type targetGenericType, Type[] sourceGenericArgs,
                                                Type[] targetGenericArgs, Type pocoType)
        {
            if (targetGenericType == typeof(SortedDictionary<,>) || targetGenericType == typeof(IDictionary<,>))
            {
                if (sourceGenericArgs[0] != targetGenericArgs[0] || sourceGenericArgs[1] != targetGenericArgs[1])
                {
                    return ConvertToSortedDictionaryFromDbMethod
                        .MakeGenericMethod(sourceGenericArgs[0], sourceGenericArgs[1], targetGenericArgs[0],
                            targetGenericArgs[1], pocoType).CreateDelegateLocal(this);
                }
            }
            if (targetGenericType == typeof(Dictionary<,>))
            {
                // Allow conversion from IDictionary<,> -> Dictionary<,> since C* driver uses
                // SortedDictionary which can't be casted into Dictionary
                if (sourceGenericArgs[0] == targetGenericArgs[0] && sourceGenericArgs[1] == targetGenericArgs[1])
                {
                    return ConvertToDictionaryMethod.MakeGenericMethod(sourceGenericArgs).CreateDelegateLocal();
                }
                return ConvertToDictionaryFromDbMethod
                    .MakeGenericMethod(sourceGenericArgs[0], sourceGenericArgs[1], targetGenericArgs[0], 
                        targetGenericArgs[1]).CreateDelegateLocal(this);
            }
            return null;
        }

        private Delegate ConvertFromIEnumerable(Type targetGenericType, Type[] sourceGenericArgs,
                                                                  Type[] targetGenericArgs, Type pocoType)
        {
            // Use equality operators to compare supported open generic types (IsAssignableFrom() won't help)
            if (targetGenericType == typeof(List<>) || targetGenericType == typeof(IList<>))
            {
                if (sourceGenericArgs[0] == targetGenericArgs[0])
                {
                    return ConvertToListMethod
                        .MakeGenericMethod(sourceGenericArgs)
                        .CreateDelegateLocal();
                }
                return ConvertToListFromDbMethod
                    .MakeGenericMethod(sourceGenericArgs[0], targetGenericArgs[0], pocoType)
                    .CreateDelegateLocal(this);
            }
            if (targetGenericType == typeof(SortedSet<>) || targetGenericType == typeof(ISet<>))
            {
                if (sourceGenericArgs[0] == targetGenericArgs[0])
                {
                    return ConvertToSortedSetMethod
                        .MakeGenericMethod(sourceGenericArgs).CreateDelegateLocal();
                }
                return ConvertToSortedSetFromDbMethod
                    .MakeGenericMethod(sourceGenericArgs[0], targetGenericArgs[0], pocoType).CreateDelegateLocal(this);
            }
            if (targetGenericType == typeof(HashSet<>))
            {
                if (sourceGenericArgs[0] == targetGenericArgs[0])
                {
                    return ConvertToHashSetMethod
                        .MakeGenericMethod(sourceGenericArgs).CreateDelegateLocal();
                }
                return ConvertToHashSetFromDbMethod
                    .MakeGenericMethod(sourceGenericArgs[0], targetGenericArgs[0]).CreateDelegateLocal(this);
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
            if (dbType == typeof(Guid) && pocoType == typeof(TimeUuid))
            {
                Guid TimeUuidConverter(TimeUuid a) => a.ToGuid();
                return (Func<TimeUuid, Guid>) TimeUuidConverter;
            }
            if (typeof(IEnumerable).GetTypeInfo().IsAssignableFrom(dbType) && dbType.GetTypeInfo().IsGenericType)
            {
                Type dbGenericType = dbType.GetTypeInfo().GetGenericTypeDefinition();
                Type[] dbTypeGenericArgs = dbType.GetTypeInfo().GetGenericArguments();
                if (pocoType.GetTypeInfo().IsArray)
                {
                    // Its an array, convert each element
                    return ConvertIEnumerableToDbTypeMethod
                        .MakeGenericMethod(pocoType.GetTypeInfo().GetElementType(), dbTypeGenericArgs[0])
                        .CreateDelegateLocal(this);
                }
                if (!pocoType.GetTypeInfo().IsGenericType || dbType.GetTypeInfo().IsAssignableFrom(pocoType))
                {
                    return null;
                }
                Type[] pocoTypeGenericArgs = pocoType.GetTypeInfo().GetGenericArguments();
                if (dbGenericType == typeof(IEnumerable<>))
                {
                    // Its a list or a set but the child types doesn't match
                    return ConvertIEnumerableToDbTypeMethod
                        .MakeGenericMethod(pocoTypeGenericArgs[0], dbTypeGenericArgs[0]).CreateDelegateLocal(this);
                }
                if (dbGenericType == typeof(IDictionary<,>))
                {
                    return ConvertIDictionaryToDbTypeMethod
                        .MakeGenericMethod(pocoTypeGenericArgs[0], pocoTypeGenericArgs[1], dbTypeGenericArgs[0],
                            dbTypeGenericArgs[1]).CreateDelegateLocal(this);
                }
            }

            return null;
        }

        private Func<TPoco, TDatabase> TryFindToDbConverter<TPoco, TDatabase>()
        {
            var converter = FindToDbConverter<TPoco, TDatabase>();
            if (converter != null)
            {
                return (Func<TPoco, TDatabase>)converter;
            }
            
            TDatabase ChangeType(TPoco a)
            {
                try
                {
                    return (TDatabase) (object) a;
                }
                catch (InvalidCastException ex)
                {
                    throw new InvalidCastException(
                        $"Specified cast is not valid: from {a.GetType()} to {typeof(TDatabase)}", ex);
                }
            }

            return ChangeType;
        }

        private IEnumerable<TResult> ConvertIEnumerableToDbType<TSource, TResult>(IEnumerable<TSource> items)
        {
            return items?.Select(TryFindToDbConverter<TSource, TResult>());
        }

        private IDictionary<TResultKey, TResultValue> ConvertIDictionaryToDbType<TSourceKey, TSourceValue, TResultKey,
            TResultValue>(IDictionary<TSourceKey, TSourceValue> map)
        {
            var keyConverter = TryFindToDbConverter<TSourceKey, TResultKey>();
            var valueConverter = TryFindToDbConverter<TSourceValue, TResultValue>();
            return map?.ToDictionary(kv => keyConverter(kv.Key), kv => valueConverter(kv.Value));
        }

        private static Dictionary<TKey, TValue> ConvertToDictionary<TKey, TValue>(IDictionary<TKey, TValue> map)
        {
            return new Dictionary<TKey, TValue>(map);
        }

        private Dictionary<TKeyResult, TValueResult> ConvertToDictionaryFromDb<TKeySource, TValueSource, TKeyResult,
            TValueResult>(IDictionary<TKeySource, TValueSource> mapFromDatabase)
        {
            var keyConverter = TryGetFromDbConverter<TKeySource, TKeyResult>();
            var valueConverter = TryGetFromDbConverter<TValueSource, TValueResult>();
            var dictionary = new Dictionary<TKeyResult, TValueResult>(mapFromDatabase.Count);
            foreach (var kv in mapFromDatabase)
            {
                dictionary.Add(keyConverter(kv.Key), valueConverter(kv.Value));
            }
            return dictionary;
        }

        private TDictionaryResult ConvertToSortedDictionaryFromDb
            <TKeySource, TValueSource, TKeyResult, TValueResult, TDictionaryResult>
            (IDictionary<TKeySource, TValueSource> mapFromDatabase)
        {
            var keyConverter = TryGetFromDbConverter<TKeySource, TKeyResult>();
            var valueConverter = TryGetFromDbConverter<TValueSource, TValueResult>();
            var dictionary = new SortedDictionary<TKeyResult, TValueResult>();
            foreach (var kv in mapFromDatabase)
            {
                dictionary.Add(keyConverter(kv.Key), valueConverter(kv.Value));
            }
            return (TDictionaryResult) (object) dictionary;
        }

        private static HashSet<T> ConvertToHashSet<T>(IEnumerable<T> set)
        {
            return new HashSet<T>(set);
        }

        private HashSet<TResult> ConvertToHashSetFromDb<TSource, TResult>(IEnumerable<TSource> setFromDatabase)
        {
            return new HashSet<TResult>(setFromDatabase.Select(TryGetFromDbConverter<TSource, TResult>()));
        }

        private static SortedSet<T> ConvertToSortedSet<T>(IEnumerable<T> set)
        {
            if (set is SortedSet<T>)
            {
                return (SortedSet<T>) set;
            }
            return new SortedSet<T>(set);
        }

        private TSetResult ConvertToSortedSetFromDb<TSource, TResult, TSetResult>(
            IEnumerable<TSource> setFromDatabase)
        {
            return (TSetResult) (object) new SortedSet<TResult>(
                setFromDatabase.Select(TryGetFromDbConverter<TSource, TResult>()));
        }

        private static List<T> ConvertToList<T>(IEnumerable<T> list)
        {
            if (list is List<T>)
            {
                return (List<T>) list;
            }
            return new List<T>(list);
        }

        private TListResult ConvertToListFromDb<TSource, TResult, TListResult>(IEnumerable<TSource> itemsDatabase)
        {
            return (TListResult) (object) new List<TResult>(
                itemsDatabase.Select(TryGetFromDbConverter<TSource, TResult>()));
        }
        
        private TResult[] ConvertToArrayFromDb<TSource, TResult>(IEnumerable<TSource> listFromDatabase)
        {
            return listFromDatabase.Select(TryGetFromDbConverter<TSource, TResult>()).ToArray();
        }

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