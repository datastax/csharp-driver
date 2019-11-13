//
//  Copyright (C) DataStax, Inc.
//
//  Please see the license for details:
//  http://www.datastax.com/terms/datastax-dse-driver-license-terms
//

using System.Collections.Generic;
using System.Threading.Tasks;

namespace Cassandra.Mapping
{
    /// <summary>
    /// A client capable of querying (reading) POCOs from a Cassandra cluster.
    /// </summary>
    public interface ICqlQueryAsyncClient
    {
        /// <summary>
        /// Gets a list of all T from Cassandra.
        /// </summary>
        Task<IEnumerable<T>> FetchAsync<T>(CqlQueryOptions queryOptions = null);
        
        /// <summary>
        /// Gets a list of T from Cassandra using the CQL statement and parameter values specified.
        /// </summary>
        Task<IEnumerable<T>> FetchAsync<T>(string cql, params object[] args);

        /// <summary>
        /// Gets a list of T from Cassandra using the CQL statement specified.
        /// </summary>
        Task<IEnumerable<T>> FetchAsync<T>(Cql cql);

        /// <summary>
        /// Gets a paged list of T results from Cassandra.
        /// Suitable for manually page through all the results of a query.
        /// </summary>
        Task<IPage<T>> FetchPageAsync<T>(Cql cql);

        /// <summary>
        /// Gets a paged list of T results from Cassandra using the CQL statement specified.
        /// Suitable for manually page through all the results of a query.
        /// </summary>
        Task<IPage<T>> FetchPageAsync<T>(CqlQueryOptions queryOptions = null);

        /// <summary>
        /// Gets a paged list of T results from Cassandra.
        /// Suitable for manually page through all the results of a query.
        /// </summary>
        /// <param name="pageSize">Amount of items to return</param>
        /// <param name="pagingState">The token representing the state of the result page. To get the first page, use a null value.</param>
        /// <param name="query">Cql query</param>
        /// <param name="args">Query parameters</param>
        /// <returns></returns>
        Task<IPage<T>> FetchPageAsync<T>(int pageSize, byte[] pagingState, string query, object[] args);

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