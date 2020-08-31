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

using System;
using System.Collections;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using System.Reflection;
using Cassandra.Collections;

namespace Cassandra.Mapping.TypeConversion
{
    /// <summary>
    /// A factory for retrieving Functions capable of converting between two Types.  To use custom Type conversions, inheritors
    /// should derive from this class and implement the <see cref="GetUserDefinedFromDbConverter{TDatabase,TPoco}"/> and
    /// <see cref="GetUserDefinedToDbConverter{TPoco,TDatabase}"/> methods.
    /// </summary>
    public abstract class TypeConverter
    {
        internal static readonly IReadOnlyCollection<Type> ListGenericInterfaces = 
            new ReadOnlyCollection<Type>(new HashSet<Type>(
                typeof(List<>)
                    .GetTypeInfo()
                    .GetInterfaces()
                    .Select(i => i.GetTypeInfo())
                    .Where(i => i.IsGenericType)
                    .Select(i => i.GetGenericTypeDefinition())));

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
        
        private static readonly MethodInfo ConvertIEnumerableToSetDbMethod = typeof(TypeConverter)
            .GetTypeInfo().GetMethod(nameof(ConvertIEnumerableToSetDb), PrivateInstance);
        
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
                throw new InvalidOperationException(
                    $"No converter is available from Type {typeof(TValue).Name} to Type {typeof(TDatabase).Name}");
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
            var converter = TryGetFromDbConverter(typeof(TSource), typeof(TResult));
            if (converter == null)
            {
                return ChangeType<TSource, TResult>;
            }

            return (Func<TSource, TResult>) converter;
        }

        private TResult ChangeType<TSource, TResult>(TSource a)
        {
            try
            {
                return (TResult)(object)a;
            }
            catch (Exception ex)
            {                    
                throw new InvalidCastException(
                    $"Specified cast is not valid: from " +
                    $"{(a == null ? $"null ({typeof(TSource)})" : a.GetType().ToString())} to {typeof(TResult)}", ex);
            }
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

            if (pocoType == dbType)
            {
                Func<TPoco, TPoco> func = d => d;
                return func;
            }
            
            if (pocoType.GetTypeInfo().IsGenericType 
                && pocoType.GetGenericTypeDefinition() == typeof(Nullable<>))
            {
                var underlyingType = Nullable.GetUnderlyingType(pocoType);
                if (underlyingType != null)
                {
                    var deleg = (Delegate)TypeConverter.FindFromDbConverterMethod.MakeGenericMethod(dbType, underlyingType).Invoke(this, null);
                    if (deleg == null)
                    {
                        return null;
                    }

                    Func<TDatabase, TPoco> mapper = d => d == null ? default(TPoco) : (TPoco)deleg.DynamicInvoke(d);
                    return mapper;
                }
            }

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

            if (dbType.GetTypeInfo().IsGenericType || dbType.GetInterfaces().Any(i => i.IsGenericType))
            {
                Type sourceEnumerableInterface = dbType.IsGenericType && dbType.GetGenericTypeDefinition() == typeof(IEnumerable<>) 
                    ? dbType 
                    : dbType.GetInterfaces().FirstOrDefault(
                        i => i.IsGenericType && i.GetGenericTypeDefinition() == typeof(IEnumerable<>));
                Type[] sourceGenericArgs = sourceEnumerableInterface != null 
                    ? sourceEnumerableInterface.GetTypeInfo().GetGenericArguments()
                    : dbType.GetTypeInfo().GetGenericArguments();
                if (pocoType.IsArray && sourceEnumerableInterface != null)
                {
                    return ConvertToArrayFromDbMethod
                        .MakeGenericMethod(sourceGenericArgs[0], pocoType.GetTypeInfo().GetElementType())
                        .CreateDelegateLocal(this);
                }
                if (pocoType.GetTypeInfo().IsGenericType)
                {
                    var targetGenericType = pocoType.GetTypeInfo().GetGenericTypeDefinition();
                    var targetGenericArgs = pocoType.GetTypeInfo().GetGenericArguments();
                    
                    Type sourceDictionaryInterface = dbType.IsGenericType && dbType.GetGenericTypeDefinition() == typeof(IDictionary<,>) 
                        ? dbType 
                        : dbType.GetInterfaces().FirstOrDefault(
                            i => i.IsGenericType && i.GetGenericTypeDefinition() == typeof(IDictionary<,>));
                    
                    sourceGenericArgs = sourceDictionaryInterface != null 
                        ? sourceDictionaryInterface.GetTypeInfo().GetGenericArguments()
                        : sourceGenericArgs;

                    if (sourceDictionaryInterface != null)
                    {
                        return ConvertFromIDictionary(targetGenericType, sourceGenericArgs, targetGenericArgs,
                            pocoType);
                    }

                    // IEnumerable<> could be a Set or a List from Cassandra
                    if (sourceEnumerableInterface != null)
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

            if (TypeConverter.ListGenericInterfaces.Contains(targetGenericType))
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

            if (typeof(TPoco) == typeof(TDatabase))
            {
                Func<TPoco, TPoco> func = d => d;
                return func;
            }
            
            if (pocoType.GetTypeInfo().IsGenericType 
                && pocoType.GetGenericTypeDefinition() == typeof(Nullable<>)
                && !dbType.GetTypeInfo().IsValueType)
            {
                var underlyingType = Nullable.GetUnderlyingType(pocoType);
                if (underlyingType != null)
                {
                    var deleg = (Delegate)TypeConverter.FindToDbConverterMethod.MakeGenericMethod(underlyingType, dbType).Invoke(this, null);
                    if (deleg == null)
                    {
                        return null;
                    }

                    Func<TPoco, TDatabase> mapper = d =>
                    {
                        if (d != null)
                        {
                            return (TDatabase) deleg.DynamicInvoke(d);
                        }

                        if (default(TDatabase) != null)
                        {
                            throw new InvalidCastException("Can not convert null value to type " + dbType.Name);
                        }

                        return default(TDatabase);

                    };
                    return mapper;
                }
            }

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
                Type[] pocoTypeGenericArgs = null;
                
                if (pocoType.GetTypeInfo().IsArray)
                {
                    pocoTypeGenericArgs = new [] { pocoType.GetTypeInfo().GetElementType() };
                }
                else if (pocoType.GetTypeInfo().IsGenericType)
                {
                    pocoTypeGenericArgs = pocoType.GetTypeInfo().GetGenericArguments();
                }

                if (pocoTypeGenericArgs == null 
                    || (dbType.GetTypeInfo().IsAssignableFrom(pocoType) 
                        && pocoTypeGenericArgs.SequenceEqual(dbTypeGenericArgs)))
                {
                    Func<TPoco, TDatabase> changeTypeDelegate = ChangeType<TPoco, TDatabase>;
                    return changeTypeDelegate;
                }
                
                if (pocoType.GetTypeInfo().IsArray || dbGenericType == typeof(IEnumerable<>))
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
                if (dbGenericType == typeof(ISet<>))
                {
                    if (pocoTypeGenericArgs[0] == dbTypeGenericArgs[0])
                    {
                        return ConvertToHashSetMethod
                               .MakeGenericMethod(pocoTypeGenericArgs).CreateDelegateLocal();
                    }
                    return ConvertIEnumerableToSetDbMethod
                           .MakeGenericMethod(pocoTypeGenericArgs[0], dbTypeGenericArgs[0]).CreateDelegateLocal(this);
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

            return ChangeType<TPoco, TDatabase>;
        }
        
        private IEnumerable<TResult> ConvertIEnumerableToDbType<TSource, TResult>(IEnumerable<TSource> items)
        {
            return items?.Select(TryFindToDbConverter<TSource, TResult>());
        }

        private IDictionary<TResultKey, TResultValue> ConvertIDictionaryToDbType<TSourceKey, TSourceValue, TResultKey,
            TResultValue>(IDictionary<TSourceKey, TSourceValue> map)
        {
            if (map == null)
            {
                return null;
            }

            var keyConverter = TryFindToDbConverter<TSourceKey, TResultKey>();
            var valueConverter = TryFindToDbConverter<TSourceValue, TResultValue>();
            return map?.ToDictionary(kv => keyConverter(kv.Key), kv => valueConverter(kv.Value));
        }

        private static Dictionary<TKey, TValue> ConvertToDictionary<TKey, TValue>(IDictionary<TKey, TValue> map)
        {
            if (map == null)
            {
                return null;
            }

            return new Dictionary<TKey, TValue>(map);
        }

        private Dictionary<TKeyResult, TValueResult> ConvertToDictionaryFromDb<TKeySource, TValueSource, TKeyResult,
            TValueResult>(IDictionary<TKeySource, TValueSource> mapFromDatabase)
        {
            if (mapFromDatabase == null)
            {
                return null;
            }

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
            if (mapFromDatabase == null)
            {
                return default(TDictionaryResult);
            }

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
            if (set == null)
            {
                return null;
            }

            return new HashSet<T>(set);
        }

        private HashSet<TResult> ConvertToHashSetFromDb<TSource, TResult>(IEnumerable<TSource> setFromDatabase)
        {
            if (setFromDatabase == null)
            {
                return null;
            }

            return new HashSet<TResult>(setFromDatabase.Select(TryGetFromDbConverter<TSource, TResult>()));
        }
        
        private HashSet<TResult> ConvertIEnumerableToSetDb<TSource, TResult>(IEnumerable<TSource> set)
        {
            if (set == null)
            {
                return null;
            }

            return new HashSet<TResult>(set.Select(TryFindToDbConverter<TSource, TResult>()));
        }

        private static SortedSet<T> ConvertToSortedSet<T>(IEnumerable<T> set)
        {
            if (set == null)
            {
                return null;
            }

            if (set is SortedSet<T>)
            {
                return (SortedSet<T>) set;
            }
            return new SortedSet<T>(set);
        }

        private TSetResult ConvertToSortedSetFromDb<TSource, TResult, TSetResult>(
            IEnumerable<TSource> setFromDatabase)
        {
            if (setFromDatabase == null && default(TSetResult) == null)
            {
                return default(TSetResult);
            }

            return (TSetResult) (object) new SortedSet<TResult>(
                setFromDatabase.Select(TryGetFromDbConverter<TSource, TResult>()));
        }

        private static List<T> ConvertToList<T>(IEnumerable<T> list)
        {
            if (list == null)
            {
                return null;
            }

            if (list is List<T>)
            {
                return (List<T>) list;
            }
            return new List<T>(list);
        }

        private TListResult ConvertToListFromDb<TSource, TResult, TListResult>(IEnumerable<TSource> itemsDatabase)
        {
            if (itemsDatabase == null)
            {
                return default(TListResult);
            }

            return (TListResult) (object) new List<TResult>(
                itemsDatabase.Select(TryGetFromDbConverter<TSource, TResult>()));
        }
        
        private TResult[] ConvertToArrayFromDb<TSource, TResult>(IEnumerable<TSource> listFromDatabase)
        {
            if (listFromDatabase == null)
            {
                return null;
            }

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