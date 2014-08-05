using System.Threading.Tasks;

namespace CqlPoco
{
    /// <summary>
    /// The client for creating, updating, deleting, and reading POCOs from a Cassandra cluster.
    /// </summary>
    public interface ICqlClient : ICqlQueryClient
    {
        /// <summary>
        /// Inserts the specified POCO in Cassandra.
        /// </summary>
        Task Insert<T>(T poco);

        /// <summary>
        /// Updates the POCO specified in Cassandra.
        /// </summary>
        Task Update<T>(T poco);

        /// <summary>
        /// Updates the table for the POCO type specified (T) using the CQL string and bind variable values specified.  Prepends "UPDATE tablename " to the CQL
        /// string you specify, getting the tablename appropriately from the POCO Type T.
        /// </summary>
        /// <typeparam name="T">The POCO Type being updated.</typeparam>
        Task Update<T>(string cql, params object[] args);

        /// <summary>
        /// Deletes the specified POCO from Cassandra.
        /// </summary>
        Task Delete<T>(T poco);

        /// <summary>
        /// Deletes from the table for the POCO type specified (T) using the CQL string and bind variable values specified.  Prepends "DELETE FROM tablname " to
        /// the CQL string you specify, getting the tablename appropriately from the POCO Type T.
        /// </summary>
        Task Delete<T>(string cql, params object[] args);

        /// <summary>
        /// Executes an arbitrary CQL string with the bind variable values specified.
        /// </summary>
        Task Execute(string cql, params object[] args);

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
