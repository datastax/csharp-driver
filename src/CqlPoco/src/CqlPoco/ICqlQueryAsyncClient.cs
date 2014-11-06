using System.Collections.Generic;
using System.Threading.Tasks;

namespace CqlPoco
{
    /// <summary>
    /// A client capable of querying (reading) POCOs from a Cassandra cluster.
    /// </summary>
    public interface ICqlQueryAsyncClient
    {
        /// <summary>
        /// Gets a list of all T from Cassandra.
        /// </summary>
        Task<List<T>> FetchAsync<T>(CqlQueryOptions queryOptions = null);
        
        /// <summary>
        /// Gets a list of T from Cassandra using the CQL statement and parameter values specified.
        /// </summary>
        Task<List<T>> FetchAsync<T>(string cql, params object[] args);

        /// <summary>
        /// Gets a list of T from Cassandra using the CQL statement specified.
        /// </summary>
        Task<List<T>> FetchAsync<T>(Cql cql);

        /// <summary>
        /// Gets a single T from Cassandra using the CQL statement and parameter values specified.  Will throw if
        /// no records or more than one record is returned.
        /// </summary>
        Task<T> SingleAsync<T>(string cql, params object[] args);

        /// <summary>
        /// Gets a single T from Cassandra using the CQL statement specified.  Will throw if no records or more than one
        /// record is returned.
        /// </summary>
        Task<T> SingleAsync<T>(Cql cql);

        /// <summary>
        /// Gets a single T from Cassandra using the CQL statement and parameter values specified.  Will return the
        /// default value of T if no records are found.  Will throw if more than one record is returned.
        /// </summary>
        Task<T> SingleOrDefaultAsync<T>(string cql, params object[] args);

        /// <summary>
        /// Gets a single T from Cassandra using the CQL statement specified.  Will return the default value of T if no
        /// records are found.  Will throw if more than one record is returned.
        /// </summary>
        Task<T> SingleOrDefaultAsync<T>(Cql cql);

        /// <summary>
        /// Gets the first T from Cassandra using the CQL statement and parameter values specified.  Will throw if no
        /// records are returned.
        /// </summary>
        Task<T> FirstAsync<T>(string cql, params object[] args);

        /// <summary>
        /// Gets the first T from Cassandra using the CQL specified.  Will throw if no records are returned.
        /// </summary>
        Task<T> FirstAsync<T>(Cql cql);

        /// <summary>
        /// Gets the first T from Cassandra using the CQL statement and parameter values specified.  Will return the
        /// default value of T is no records are found.
        /// </summary>
        Task<T> FirstOrDefaultAsync<T>(string cql, params object[] args);

        /// <summary>
        /// Gets the first T from Cassandra using the CQL specified.  Will return the default value of T if no records
        /// are found.
        /// </summary>
        Task<T> FirstOrDefaultAsync<T>(Cql cql);
    }
}