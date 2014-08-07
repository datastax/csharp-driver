using System.Collections.Generic;

namespace CqlPoco
{
    /// <summary>
    /// The contract for synchronous read operations.
    /// </summary>
    public interface ICqlQueryClient
    {
        /// <summary>
        /// Gets a list of all T from Cassandra.
        /// </summary>
        List<T> Fetch<T>(CqlQueryOptions queryOptions = null);

        /// <summary>
        /// Gets a list of T from Cassandra using the CQL statement and parameter values specified.
        /// </summary>
        List<T> Fetch<T>(string cql, params object[] args);

        /// <summary>
        /// Gets a list of T from Cassandra using the CQL statement specified.
        /// </summary>
        List<T> Fetch<T>(Cql cql);
        
        /// <summary>
        /// Gets a single T from Cassandra using the CQL statement and parameter values specified.  Will throw if
        /// no records or more than one record is returned.
        /// </summary>
        T Single<T>(string cql, params object[] args);

        /// <summary>
        /// Gets a single T from Cassandra using the CQL statement specified.  Will throw if no records or more than
        /// one record is returned.
        /// </summary>
        T Single<T>(Cql cql);

        /// <summary>
        /// Gets a single T from Cassandra using the CQL statement and parameter values specified.  Will return the
        /// default value of T if no records are found.  Will throw if more than one record is returned.
        /// </summary>
        T SingleOrDefault<T>(string cql, params object[] args);

        /// <summary>
        /// Gets a single T from Cassandra using the CQL statement specified.  Will return the default value of T if
        /// no records are found.  Will throw if more than one record is returned.
        /// </summary>
        T SingleOrDefault<T>(Cql cql);

        /// <summary>
        /// Gets the first T from Cassandra using the CQL statement and parameter values specified.  Will throw if no
        /// records are returned.
        /// </summary>
        T First<T>(string cql, params object[] args);

        /// <summary>
        /// Gets the first T from Cassandra using the CQL statement specified.  Will throw if no records are returned.
        /// </summary>
        T First<T>(Cql cql);

        /// <summary>
        /// Gets the first T from Cassandra using the CQL statement and parameter values specified.  Will return the
        /// default value of T is no records are found.
        /// </summary>
        T FirstOrDefault<T>(string cql, params object[] args);

        /// <summary>
        /// Gets the first T from Cassandra using the CQL statement specified.  Will return the default value of T if
        /// no records are found.
        /// </summary>
        T FirstOrDefault<T>(Cql cql);
    }
}