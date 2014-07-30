using System;
using System.Collections.Concurrent;
using System.Reflection;

namespace CqlPoco.TypeConversion
{
    /// <summary>
    /// A factory for retrieving Functions capable of converting between two Types.  To use custom Type conversions, inheritors
    /// should derive from this class and implement the GetUserDefinedFromDbConverter method.
    /// </summary>
    public abstract class TypeConverterFactory
    {
        private static readonly MethodInfo FindFromDbConverterMethod = typeof (TypeConverterFactory).GetMethod("FindFromDbConverter",
                                                                                                               BindingFlags.NonPublic |
                                                                                                               BindingFlags.Instance);

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
        private Delegate FindFromDbConverter<TSource, TDest>()
        {
            // Allow for user-defined conversions
            Delegate converter = GetUserDefinedFromDbConverter<TSource, TDest>();
            if (converter != null)
                return converter;

            // Allow strings from the database to be converted to an enum property on a POCO
            if (typeof (TSource) == typeof(string) && typeof (TDest).IsEnum)
            {
                Func<string, TDest> enumMapper = EnumStringMapper<TDest>.MapStringToEnum;
                return enumMapper;
            }

            return null;
        }
        
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