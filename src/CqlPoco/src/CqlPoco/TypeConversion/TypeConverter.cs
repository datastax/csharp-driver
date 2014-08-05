using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Reflection;

namespace CqlPoco.TypeConversion
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

        private static readonly MethodInfo FindFromDbConverterMethod = typeof (TypeConverter).GetMethod("FindFromDbConverter", PrivateInstance);

        private static readonly MethodInfo FindToDbConverterMethod = typeof (TypeConverter).GetMethod("FindToDbConverter", PrivateInstance);

        private static readonly MethodInfo ConvertToDictionaryMethod = typeof (TypeConverter).GetMethod("ConvertToDictionary", PrivateStatic);

        private static readonly MethodInfo ConvertToHashSetMethod = typeof(TypeConverter).GetMethod("ConvertToHashSet", PrivateStatic);

        private static readonly MethodInfo ConvertToSortedSetMethod = typeof(TypeConverter).GetMethod("ConvertToSortedSet", PrivateStatic);

        private static readonly MethodInfo ConvertToArrayMethod = typeof (Enumerable).GetMethod("ToArray", BindingFlags.Public | BindingFlags.Static);

        private readonly ConcurrentDictionary<Tuple<Type, Type>, Delegate> _fromDbConverterCache;
        private readonly ConcurrentDictionary<Tuple<Type, Type>, Delegate> _toDbConverterCache; 

        protected TypeConverter()
        {
            _fromDbConverterCache = new ConcurrentDictionary<Tuple<Type, Type>, Delegate>();
            _toDbConverterCache = new ConcurrentDictionary<Tuple<Type, Type>, Delegate>();
        }

        /// <summary>
        /// Converts a value of Type <see cref="TValue"/> to a value of Type <see cref="TDatabase"/> using any available converters that would normally be used
        /// when converting a value for storage in Cassandra.  If no converter is available, wlll throw an InvalidOperationException.
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
            return _fromDbConverterCache.GetOrAdd(Tuple.Create(dbType, pocoType),
                                                  // Invoke the generic method below with our two type parameters
                                                  _ => (Delegate) FindFromDbConverterMethod.MakeGenericMethod(dbType, pocoType).Invoke(this, null));
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
                if (pocoType.IsEnum)
                {
                    Func<string, TPoco> enumMapper = EnumStringMapper<TPoco>.MapStringToEnum;
                    return enumMapper;
                }

                var underlyingPocoType = Nullable.GetUnderlyingType(pocoType);
                if (underlyingPocoType != null && underlyingPocoType.IsEnum)
                {
                    Func<string, TPoco> enumMapper = NullableEnumStringMapper<TPoco>.MapStringToEnum;
                    return enumMapper;
                }
            }

            if (dbType.IsGenericType && pocoType.IsGenericType)
            {
                Type sourceGenericDefinition = dbType.GetGenericTypeDefinition();
                Type[] sourceGenericArgs = dbType.GetGenericArguments();

                // Allow conversion from IDictionary<,> -> Dictionary<,> since C* driver uses SortedDictionary which can't be cast to Dictionary
                if (sourceGenericDefinition == typeof (IDictionary<,>) && pocoType == typeof (Dictionary<,>).MakeGenericType(sourceGenericArgs))
                {
                    return ConvertToDictionaryMethod.MakeGenericMethod(sourceGenericArgs).CreateDelegate(typeof (Func<TDatabase, TPoco>));
                }

                // IEnumerable<> could be a Set or a List from Cassandra
                if (sourceGenericDefinition == typeof (IEnumerable<>))
                {
                    // For some reason, the driver uses List<> to represent Sets so allow conversion to HashSet<>, SortedSet<>, and ISet<>
                    if (pocoType == typeof (HashSet<>).MakeGenericType(sourceGenericArgs))
                    {
                        return ConvertToHashSetMethod.MakeGenericMethod(sourceGenericArgs).CreateDelegate(typeof (Func<TDatabase, TPoco>));
                    }

                    if (pocoType == typeof (SortedSet<>).MakeGenericType(sourceGenericArgs) ||
                        pocoType == typeof (ISet<>).MakeGenericType(sourceGenericArgs))
                    {
                        return ConvertToSortedSetMethod.MakeGenericMethod(sourceGenericArgs).CreateDelegate(typeof (Func<TDatabase, TPoco>));
                    }

                    // Allow converting from set/list's IEnumerable<T> to T[]
                    if (pocoType == sourceGenericArgs[0].MakeArrayType())
                    {
                        return ConvertToArrayMethod.MakeGenericMethod(sourceGenericArgs).CreateDelegate(typeof (Func<TDatabase, TPoco>));
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
                return converter;

            Type pocoType = typeof (TPoco);
            Type dbType = typeof (TDatabase);

            // Support enum/nullable enum => string conversion
            if (dbType == typeof (string))
            {
                if (pocoType.IsEnum)
                {
                    // Just call ToStirng() on the enum value from the POCO
                    Func<TPoco, string> enumConverter = prop => prop.ToString();
                    return enumConverter;
                }

                Type underlyingPocoType = Nullable.GetUnderlyingType(pocoType);
                if (underlyingPocoType != null && underlyingPocoType.IsEnum)
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
        /// Gets any user defined conversion functions that can convert a value of type <see cref="TDatabase"/> (coming from Cassandra) to a
        /// type of <see cref="TPoco"/> (a field or property on a POCO).  Return null if no conversion Func is available.
        /// </summary>
        /// <typeparam name="TDatabase">The Type of the source value from Cassandra to be converted.</typeparam>
        /// <typeparam name="TPoco">The Type of the destination value on the POCO.</typeparam>
        /// <returns>A Func that can convert between the two types or null if one is not available.</returns>
        protected abstract Func<TDatabase, TPoco> GetUserDefinedFromDbConverter<TDatabase, TPoco>();

        /// <summary>
        /// Gets any user defined conversion functions that can convert a value of type <see cref="TPoco"/> (coming from a property/field on a
        /// POCO) to a type of <see cref="TDatabase"/> (the Type expected by Cassandra for the database column).  Return null if no conversion
        /// Func is available.
        /// </summary>
        /// <typeparam name="TPoco">The Type of the source value from the POCO property/field to be converted.</typeparam>
        /// <typeparam name="TDatabase">The Type expected by C* for the database column.</typeparam>
        /// <returns>A Func that can converter between the two Types or null if one is not available.</returns>
        protected abstract Func<TPoco, TDatabase> GetUserDefinedToDbConverter<TPoco, TDatabase>();
    }
}