﻿using System.Threading.Tasks;

namespace CqlPoco
{
    /// <summary>
    /// The contract for Async write operations.
    /// </summary>
    public interface ICqlWriteAsyncClient
    {
        /// <summary>
        /// Inserts the specified POCO in Cassandra.
        /// </summary>
        Task InsertAsync<T>(T poco, CqlQueryOptions queryOptions = null);

        /// <summary>
        /// Updates the POCO specified in Cassandra.
        /// </summary>
        Task UpdateAsync<T>(T poco, CqlQueryOptions queryOptions = null);

        /// <summary>
        /// Updates the table for the POCO type specified (T) using the CQL string and bind variable values specified.  Prepends "UPDATE tablename " to the CQL
        /// string you specify, getting the tablename appropriately from the POCO Type T.
        /// </summary>
        /// <typeparam name="T">The POCO Type being updated.</typeparam>
        Task UpdateAsync<T>(string cql, params object[] args);

        /// <summary>
        /// Updates the table for the poco type specified (T) using the CQL statement specified.  Prepends "UPDATE tablename" to the CQL statement you specify,
        /// getting the tablename appropriately from the POCO Type T.
        /// </summary>
        Task UpdateAsync<T>(Cql cql);

        /// <summary>
        /// Deletes the specified POCO from Cassandra.
        /// </summary>
        Task DeleteAsync<T>(T poco, CqlQueryOptions queryOptions = null);

        /// <summary>
        /// Deletes from the table for the POCO type specified (T) using the CQL string and bind variable values specified.  Prepends "DELETE FROM tablname " to
        /// the CQL string you specify, getting the tablename appropriately from the POCO Type T.
        /// </summary>
        Task DeleteAsync<T>(string cql, params object[] args);

        /// <summary>
        /// Deletes from the table for the POCO type specified (T) using the CQL statement specified.  Prepends "DELETE FROM tablename " to the CQL statement you
        /// specify, getting the tablename appropriately from the POCO Type T.
        /// </summary>
        Task DeleteAsync<T>(Cql cql);

        /// <summary>
        /// Executes an arbitrary CQL string with the bind variable values specified.
        /// </summary>
        Task ExecuteAsync(string cql, params object[] args);

        /// <summary>
        /// Executes an arbitrary CQL statement.
        /// </summary>
        Task ExecuteAsync(Cql cql);
    }
}