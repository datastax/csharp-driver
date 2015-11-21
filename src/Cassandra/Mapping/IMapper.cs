using System.Threading.Tasks;

namespace Cassandra.Mapping
{
    /// <summary>
    /// A client for creating, updating, deleting, and reading POCOs from a Cassandra cluster.
    /// </summary>
    /// <seealso cref="Mapper"/>
    public interface IMapper : ICqlQueryAsyncClient, ICqlWriteAsyncClient, ICqlQueryClient, ICqlWriteClient
    {
        /// <summary>
        /// Creates a new batch.
        /// </summary>
        ICqlBatch CreateBatch();

        /// <summary>
        /// Executes the batch specfied synchronously.
        /// </summary>
        void Execute(ICqlBatch batch);

        /// <summary>
        /// Executes the batch specified asynchronously.
        /// </summary>
        Task ExecuteAsync(ICqlBatch batch);

        /// <summary>
        /// Allows you to convert an argument/bind variable value being used in a CQL statement using the same converters that are being used by the client
        /// internally, including any user-defined conversions if you configured them.  Will convert a value of Type <typeparamref name="TValue"/> to a value of
        /// Type <typeparamref name="TDatabase"/> or throw an InvalidOperationException if no converter is available.
        /// </summary>
        /// <typeparam name="TValue">The original Type of the value.</typeparam>
        /// <typeparam name="TDatabase">The Type expected by Cassandra to convert to.</typeparam>
        /// <param name="value">The value to convert.</param>
        /// <returns>The converted value.</returns>
        TDatabase ConvertCqlArgument<TValue, TDatabase>(TValue value);

        //Lightweight transaction support methods must be included at IMapper level as 
        //  conditional queries are not supported in batches

        /// <summary>
        /// Deletes from the table for the POCO type specified (T) using the CQL string specified and query parameters specified.  
        /// Prepends "DELETE FROM tablename " to the CQL statement you specify, getting the tablename appropriately from the POCO Type T.
        /// <para>
        /// Returns information whether it was applied or not. If it was not applied, it returns details of the existing values.
        /// </para>
        /// </summary>
        AppliedInfo<T> DeleteIf<T>(string cql, params object[] args);

        /// <summary>
        /// Deletes from the table for the POCO type specified (T) using the Cql query specified.  
        /// Prepends "DELETE FROM tablename " to the CQL statement you specify, getting the tablename appropriately from the POCO Type T.
        /// <para>
        /// Returns information whether it was applied or not. If it was not applied, it returns details of the existing values.
        /// </para>
        /// </summary>
        AppliedInfo<T> DeleteIf<T>(Cql cql);

        /// <summary>
        /// Deletes from the table for the POCO type specified (T) using the CQL string specified and query parameters specified.  
        /// Prepends "DELETE FROM tablename " to the CQL statement you specify, getting the tablename appropriately from the POCO Type T.
        /// <para>
        /// Returns information whether it was applied or not. If it was not applied, it returns details of the existing values.
        /// </para>
        /// </summary>
        Task<AppliedInfo<T>> DeleteIfAsync<T>(string cql, params object[] args);

        /// <summary>
        /// Deletes from the table for the POCO type specified (T) using the Cql query specified.  
        /// Prepends "DELETE FROM tablename " to the CQL statement you specify, getting the tablename appropriately from the POCO Type T.
        /// <para>
        /// Returns information whether it was applied or not. If it was not applied, it returns details of the existing values.
        /// </para>
        /// </summary>
        Task<AppliedInfo<T>> DeleteIfAsync<T>(Cql cql);

        /// <summary>
        /// Inserts the specified POCO in Cassandra, if not exists.
        /// <para>
        /// Returns information whether it was applied or not. If it was not applied, it returns details of the existing values.
        /// </para>
        /// </summary>
        Task<AppliedInfo<T>> InsertIfNotExistsAsync<T>(T poco, CqlQueryOptions queryOptions = null, CqlInsertOptions insertOptions = null);


        /// <summary>
        /// Inserts the specified POCO in Cassandra, if not exists.
        /// <para>
        /// Returns information whether it was applied or not. If it was not applied, it returns details of the existing values.
        /// </para>
        /// </summary>
        Task<AppliedInfo<T>> InsertIfNotExistsAsync<T>(T poco, CqlInsertOptions insertOptions = null);

        /// <summary>
        /// Inserts the specified POCO in Cassandra, if not exists.
        /// <para>
        /// Returns information whether it was applied or not. If it was not applied, it returns details of the existing values.
        /// </para>
        /// </summary>
        AppliedInfo<T> InsertIfNotExists<T>(T poco, CqlQueryOptions queryOptions = null, CqlInsertOptions insertOptions = null);


        /// <summary>
        /// Inserts the specified POCO in Cassandra, if not exists.
        /// <para>
        /// Returns information whether it was applied or not. If it was not applied, it returns details of the existing values.
        /// </para>
        /// </summary>
        AppliedInfo<T> InsertIfNotExists<T>(T poco, CqlInsertOptions insertOptions);

        /// <summary>
        /// Updates the table for the poco type specified (T) using the CQL statement specified, using lightweight transactions.
        /// Prepends "UPDATE tablename" to the CQL statement you specify, getting the table name appropriately from the POCO Type T.
        /// <para>
        /// Returns information whether it was applied or not. If it was not applied, it returns details of the existing values.
        /// </para>
        /// </summary>
        AppliedInfo<T> UpdateIf<T>(Cql cql);

        /// <summary>
        /// Updates the table for the poco type specified (T) using the CQL statement specified, using lightweight transactions.
        /// Prepends "UPDATE tablename" to the CQL statement you specify, getting the table name appropriately from the POCO Type T.
        /// <para>
        /// Returns information whether it was applied or not. If it was not applied, it returns details of the existing values.
        /// </para>
        /// </summary>
        AppliedInfo<T> UpdateIf<T>(string cql, params object[] args);

        /// <summary>
        /// Updates the table for the poco type specified (T) using the CQL statement specified, using lightweight transactions.
        /// Prepends "UPDATE tablename" to the CQL statement you specify, getting the table name appropriately from the POCO Type T.
        /// <para>
        /// Returns information whether it was applied or not. If it was not applied, it returns details of the existing values.
        /// </para>
        /// </summary>
        Task<AppliedInfo<T>> UpdateIfAsync<T>(Cql cql);

        /// <summary>
        /// Updates the table for the poco type specified (T) using the CQL statement specified, using lightweight transactions.
        /// Prepends "UPDATE tablename" to the CQL statement you specify, getting the table name appropriately from the POCO Type T.
        /// <para>
        /// Returns information whether it was applied or not. If it was not applied, it returns details of the existing values.
        /// </para>
        /// </summary>
        Task<AppliedInfo<T>> UpdateIfAsync<T>(string cql, params object[] args);

        /// <summary>
        /// Executes a batch that contains a Lightweight transaction. 
        /// </summary>
        /// <para>
        /// Returns information whether it was applied or not. If it was not applied, it returns details of the existing values.
        /// </para>
        Task<AppliedInfo<T>> ExecuteConditionalAsync<T>(ICqlBatch batch);

        /// <summary>
        /// Executes a batch that contains a Lightweight transaction. 
        /// </summary>
        /// <para>
        /// Returns information whether it was applied or not. If it was not applied, it returns details of the existing values.
        /// </para>
        AppliedInfo<T> ExecuteConditional<T>(ICqlBatch batch);
    }
}
