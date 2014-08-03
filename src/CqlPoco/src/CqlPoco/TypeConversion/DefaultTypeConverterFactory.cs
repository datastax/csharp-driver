using System;

namespace CqlPoco.TypeConversion
{
    /// <summary>
    /// A default implementation of TypeConversionFactory that doesn't do any user defined conversions.
    /// </summary>
    public class DefaultTypeConverterFactory : TypeConverterFactory
    {
        protected override Func<TSource, TDest> GetUserDefinedFromDbConverter<TSource, TDest>()
        {
            return null;
        }

        protected override Func<TPoco, TDatabase> GetUserDefinedToDbConverter<TPoco, TDatabase>()
        {
            return null;
        }
    }
}