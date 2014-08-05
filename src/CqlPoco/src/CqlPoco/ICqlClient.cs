namespace CqlPoco
{
    /// <summary>
    /// A client for creating, updating, deleting, and reading POCOs from a Cassandra cluster.
    /// </summary>
    public interface ICqlClient : ICqlQueryAsyncClient, ICqlWriteAsyncClient, ICqlQueryClient, ICqlWriteClient
    {
        /// <summary>
        /// Allows you to convert an argument/bind variable value being used in a CQL statement using the same converters that are being used by the client
        /// internally, including any user-defined conversions if you configured them.  Will convert a value of Type <see cref="TValue"/> to a value of
        /// Type <see cref="TDatabase"/> or throw an InvalidOperationException if no converter is available.
        /// </summary>
        /// <typeparam name="TValue">The original Type of the value.</typeparam>
        /// <typeparam name="TDatabase">The Type expected by Cassandra to convert to.</typeparam>
        /// <param name="value">The value to convert.</param>
        /// <returns>The converted value.</returns>
        TDatabase ConvertCqlArgument<TValue, TDatabase>(TValue value);
    }
}
