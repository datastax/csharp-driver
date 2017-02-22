//
//  Copyright (C) 2017 DataStax, Inc.
//
//  Please see the license for details:
//  http://www.datastax.com/terms/datastax-dse-driver-license-terms
//

using System.Collections.Generic;

namespace Dse.Mapping
{
    /// <summary>
    /// The contract for synchronous read operations.
    /// </summary>
    public interface ICqlQueryClient
    {
        /// <summary>
        /// Gets a list of all T from Cassandra.
        /// </summary>
        IEnumerable<T> Fetch<T>(CqlQueryOptions queryOptions = null);

        /// <summary>
        /// Gets a list of T from Cassandra using the CQL statement and parameter values specified.
        /// </summary>
        IEnumerable<T> Fetch<T>(string cql, params object[] args);

        /// <summary>
        /// Gets a list of T from Cassandra using the CQL statement specified.
        /// </summary>
        IEnumerable<T> Fetch<T>(Cql cql);

        /// <summary>
        /// Gets a paged list of T results from Cassandra.
        /// Suitable for manually page through all the results of a query.
        /// </summary>
        IPage<T> FetchPage<T>(Cql cql);

        /// <summary>
        /// Gets a paged list of T results from Cassandra using the CQL statement specified.
        /// Suitable for manually page through all the results of a query.
        /// </summary>
        IPage<T> FetchPage<T>(CqlQueryOptions queryOptions = null);

        /// <summary>
        /// Gets a paged list of T results from Cassandra.
        /// Suitable for manually page through all the results of a query.
        /// </summary>
        /// <param name="pageSize">Amount of items to return</param>
        /// <param name="pagingState">The token representing the state of the result page. To get the first page, use a null value.</param>
        /// <param name="query">Cql query</param>
        /// <param name="args">Query parameters</param>
        IPage<T> FetchPage<T>(int pageSize, byte[] pagingState, string query, object[] args);
        
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