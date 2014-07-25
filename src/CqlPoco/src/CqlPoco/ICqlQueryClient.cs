using System.Collections.Generic;
using System.Threading.Tasks;

namespace CqlPoco
{
    /// <summary>
    /// A client capable of querying (reading) POCOs from a Cassandra cluster.
    /// </summary>
    public interface ICqlQueryClient
    {
        /// <summary>
        /// Gets a list of T from Cassandra.
        /// </summary>
        Task<List<T>> Fetch<T>();

        /// <summary>
        /// Gets a list of T from Cassandra using the CQL statement and parameter values specified.
        /// </summary>
        Task<List<T>> Fetch<T>(string cql, params object[] args);

        /// <summary>
        /// Gets a single T from Cassandra using the CQL statement and parameter values specified.  Will throw if
        /// no records or more than on record is returned.
        /// </summary>
        Task<T> Single<T>(string cql, params object[] args);

        /// <summary>
        /// Gets a single T from Cassandra using the CQL statement and parameter values specified.  Will return the
        /// default value of T if no records are found.  Will throw if more than one record is returned.
        /// </summary>
        Task<T> SingleOrDefault<T>(string cql, params object[] args);

        /// <summary>
        /// Gets the first T from Cassandra using the CQL statement and parameter values specified.  Will throw if no
        /// records are returned.
        /// </summary>
        Task<T> First<T>(string cql, params object[] args);

        /// <summary>
        /// Gets the first T from Cassandra using the CQL statement and parameter values specified.  Will return the
        /// default value of T is no records are found.
        /// </summary>
        Task<T> FirstOrDefault<T>(string cql, params object[] args);
    }
}