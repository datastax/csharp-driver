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
        /// <para>
        /// To set the consistency level, timestamp and other batch options, use
        /// <see cref="ICqlBatch.WithOptions(System.Action{CqlQueryOptions})"/>. Individual options for each
        /// query within the batch will be ignored.
        /// </para>
        /// </summary>
        ICqlBatch CreateBatch();

        /// <summary>
        /// Creates a new batch.
        /// <para>
        /// To set the consistency level, timestamp and other batch options, use
        /// <see cref="ICqlBatch.WithOptions(System.Action{CqlQueryOptions})"/>. Individual options for each
        /// query within the batch will be ignored.
        /// </para>
        /// </summary>
        ICqlBatch CreateBatch(BatchType batchType);

        /// <summary>
        /// Executes the batch specfied synchronously.
        /// </summary>
        /// <remarks>
        /// To set the consistency level, timestamp and other batch options, use
        /// <see cref="ICqlBatch.WithOptions(System.Action{CqlQueryOptions})"/>. Individual options for each
        /// query within the batch will be ignored.
        /// </remarks>
        void Execute(ICqlBatch batch);

        /// <summary>
        /// Executes the batch specified asynchronously.
        /// </summary>
        /// <remarks>
        /// To set the consistency level, timestamp and other batch options, use
        /// <see cref="ICqlBatch.WithOptions(System.Action{CqlQueryOptions})"/>. Individual options for each
        /// query within the batch will be ignored.
        /// </remarks>
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

        ////Execution Profiles should be included at IMapper level as they are not supported in individual statements within batches.

        /// <summary>
        /// Inserts the specified POCO in Cassandra using the provided execution profile.
        /// </summary>
        void Insert<T>(T poco, string executionProfile, CqlQueryOptions queryOptions = null);
        
        /// <summary>
        /// Inserts the specified POCO in Cassandra using the provided execution profile.
        /// </summary>
        /// <param name="poco">The POCO instance</param>
        /// <param name="executionProfile">The execution profile to use when executing the request.</param>
        /// <param name="insertNulls">
        /// Determines if the query must be generated using <c>NULL</c> values for <c>null</c> POCO
        /// members. 
        /// <para>
        /// Use <c>false</c> if you don't want to consider <c>null</c> values for the INSERT 
        /// operation (recommended).
        /// </para> 
        /// <para>
        /// Use <c>true</c> if you want to override all the values in the table,
        /// generating tombstones for null values.
        /// </para>
        /// </param>
        /// <param name="queryOptions">Optional query options</param>
        /// <returns></returns>
        void Insert<T>(T poco, string executionProfile, bool insertNulls, CqlQueryOptions queryOptions = null);
        
        /// <summary>
        /// Inserts the specified POCO in Cassandra using the provided execution profile.
        /// </summary>
        /// <param name="poco">The POCO instance</param>
        /// <param name="executionProfile">The execution profile to use when executing the request.</param>
        /// <param name="insertNulls">
        /// Determines if the query must be generated using <c>NULL</c> values for <c>null</c> POCO
        /// members. 
        /// <para>
        /// Use <c>false</c> if you don't want to consider <c>null</c> values for the INSERT 
        /// operation (recommended).
        /// </para> 
        /// <para>
        /// Use <c>true</c> if you want to override all the values in the table,
        /// generating tombstones for null values.
        /// </para>
        /// </param>
        /// <param name="queryOptions">Optional query options</param>
        /// <param name="ttl">Time to live (in seconds) for the inserted values. If set, the inserted values are automatically removed
        /// from the database after the specified time.</param>
        /// <returns></returns>
        void Insert<T>(T poco, string executionProfile, bool insertNulls, int? ttl, CqlQueryOptions queryOptions = null);
        
        /// <summary>
        /// Inserts the specified POCO in Cassandra using the provided execution profile.
        /// </summary>
        Task InsertAsync<T>(T poco, string executionProfile, CqlQueryOptions queryOptions = null);

        /// <summary>
        /// Inserts the specified POCO in Cassandra using the provided execution profile.
        /// </summary>
        /// <param name="poco">The POCO instance</param>
        /// <param name="executionProfile">The execution profile to use when executing the request.</param>
        /// <param name="insertNulls">
        /// Determines if the query must be generated using <c>NULL</c> values for <c>null</c> POCO
        /// members. 
        /// <para>
        /// Use <c>false</c> if you don't want to consider <c>null</c> values for the INSERT 
        /// operation (recommended).
        /// </para> 
        /// <para>
        /// Use <c>true</c> if you want to override all the values in the table,
        /// generating tombstones for null values.
        /// </para>
        /// </param>
        /// <param name="ttl">Time to live (in seconds) for the inserted values. If set, the inserted values are automatically removed
        /// from the database after the specified time.</param>
        /// <param name="queryOptions">Optional query options</param>
        /// <returns></returns>
        Task InsertAsync<T>(T poco, string executionProfile, bool insertNulls, int? ttl, CqlQueryOptions queryOptions = null);

        /// <summary>
        /// Inserts the specified POCO in Cassandra using the provided execution profile.
        /// </summary>
        /// <param name="poco">The POCO instance</param>
        /// <param name="executionProfile">The execution profile to use when executing the request.</param>
        /// <param name="insertNulls">
        /// Determines if the query must be generated using <c>NULL</c> values for <c>null</c> POCO
        /// members. 
        /// <para>
        /// Use <c>false</c> if you don't want to consider <c>null</c> values for the INSERT 
        /// operation (recommended).
        /// </para> 
        /// <para>
        /// Use <c>true</c> if you want to override all the values in the table,
        /// generating tombstones for null values.
        /// </para>
        /// </param>
        /// <param name="queryOptions">Optional query options</param>
        /// <returns></returns>
        Task InsertAsync<T>(T poco, string executionProfile, bool insertNulls, CqlQueryOptions queryOptions = null);

        ////Lightweight transaction support methods must be included at IMapper level as conditional queries are not supported in batches

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
        Task<AppliedInfo<T>> InsertIfNotExistsAsync<T>(T poco, CqlQueryOptions queryOptions = null);
        
        /// <summary>
        /// Inserts the specified POCO in Cassandra, if not exists, with the provided execution profile.
        /// <para>
        /// Returns information whether it was applied or not. If it was not applied, it returns details of the existing values.
        /// </para>
        /// </summary>
        Task<AppliedInfo<T>> InsertIfNotExistsAsync<T>(T poco, string executionProfile, CqlQueryOptions queryOptions = null);

        /// <summary>
        /// Inserts the specified POCO in Cassandra, if not exists.
        /// </summary>
        /// <param name="poco">The POCO instance</param>
        /// <param name="insertNulls">
        /// Determines if the query must be generated using <c>NULL</c> values for <c>null</c> POCO
        /// members. 
        /// <para>
        /// Use <c>false</c> if you don't want to consider <c>null</c> values for the INSERT
        /// operation (recommended).
        /// </para> 
        /// <para>
        /// Use <c>true</c> if you want to override all the values in the table,
        /// generating tombstones for null values.
        /// </para>
        /// </param>
        /// <param name="queryOptions">Optional query options</param>
        /// <returns></returns>
        Task<AppliedInfo<T>> InsertIfNotExistsAsync<T>(T poco, bool insertNulls, CqlQueryOptions queryOptions = null);
        
        /// <summary>
        /// Inserts the specified POCO in Cassandra, if not exists, with the provided execution profile.
        /// </summary>
        /// <param name="poco">The POCO instance</param>
        /// <param name="executionProfile">The execution profile to use when executing the request.</param>
        /// <param name="insertNulls">
        /// Determines if the query must be generated using <c>NULL</c> values for <c>null</c> POCO
        /// members. 
        /// <para>
        /// Use <c>false</c> if you don't want to consider <c>null</c> values for the INSERT
        /// operation (recommended).
        /// </para> 
        /// <para>
        /// Use <c>true</c> if you want to override all the values in the table,
        /// generating tombstones for null values.
        /// </para>
        /// </param>
        /// <param name="queryOptions">Optional query options</param>
        /// <returns></returns>
        Task<AppliedInfo<T>> InsertIfNotExistsAsync<T>(T poco, string executionProfile, bool insertNulls, CqlQueryOptions queryOptions = null);

        /// <summary>
        /// Inserts the specified POCO in Cassandra, if not exists.
        /// </summary>
        /// <param name="poco">The POCO instance</param>
        /// <param name="insertNulls">
        /// Determines if the query must be generated using <c>NULL</c> values for <c>null</c> POCO
        /// members. 
        /// <para>
        /// Use <c>false</c> if you don't want to consider <c>null</c> values for the INSERT
        /// operation (recommended).
        /// </para> 
        /// <para>
        /// Use <c>true</c> if you want to override all the values in the table,
        /// generating tombstones for null values.
        /// </para>
        /// </param>
        /// <param name="ttl">Time to live (in seconds) for the inserted values. If set, the inserted values are automatically removed
        /// from the database after the specified time.</param>
        /// <param name="queryOptions">Optional query options</param>
        /// <returns></returns>
        Task<AppliedInfo<T>> InsertIfNotExistsAsync<T>(T poco, bool insertNulls, int? ttl, CqlQueryOptions queryOptions = null);

        /// <summary>
        /// Inserts the specified POCO in Cassandra, if not exists, with the provided execution profile.
        /// </summary>
        /// <param name="poco">The POCO instance</param>
        /// <param name="executionProfile">The execution profile to use when executing the request.</param>
        /// <param name="insertNulls">
        /// Determines if the query must be generated using <c>NULL</c> values for <c>null</c> POCO
        /// members. 
        /// <para>
        /// Use <c>false</c> if you don't want to consider <c>null</c> values for the INSERT
        /// operation (recommended).
        /// </para> 
        /// <para>
        /// Use <c>true</c> if you want to override all the values in the table,
        /// generating tombstones for null values.
        /// </para>
        /// </param>
        /// <param name="ttl">Time to live (in seconds) for the inserted values. If set, the inserted values are automatically removed
        /// from the database after the specified time.</param>
        /// <param name="queryOptions">Optional query options</param>
        /// <returns></returns>
        Task<AppliedInfo<T>> InsertIfNotExistsAsync<T>(T poco, string executionProfile, bool insertNulls, int? ttl, CqlQueryOptions queryOptions = null);

        /// <summary>
        /// Inserts the specified POCO in Cassandra, if not exists.
        /// <para>
        /// Returns information whether it was applied or not. If it was not applied, it returns details of the existing values.
        /// </para>
        /// </summary>
        AppliedInfo<T> InsertIfNotExists<T>(T poco, CqlQueryOptions queryOptions = null);
        
        /// <summary>
        /// Inserts the specified POCO in Cassandra, if not exists, with the provided execution profile.
        /// <para>
        /// Returns information whether it was applied or not. If it was not applied, it returns details of the existing values.
        /// </para>
        /// </summary>
        AppliedInfo<T> InsertIfNotExists<T>(T poco, string executionProfile, CqlQueryOptions queryOptions = null);

        /// <summary>
        /// Inserts the specified POCO in Cassandra, if not exists.
        /// </summary>
        /// <param name="poco">The POCO instance</param>
        /// <param name="insertNulls">
        /// Determines if the query must be generated using <c>NULL</c> values for <c>null</c> POCO
        /// members. 
        /// <para>
        /// Use <c>false</c> if you don't want to consider <c>null</c> values for the INSERT
        /// operation (recommended).
        /// </para> 
        /// <para>
        /// Use <c>true</c> if you want to override all the values in the table,
        /// generating tombstones for null values.
        /// </para>
        /// </param>
        /// <param name="queryOptions">Optional query options</param>
        /// <returns></returns>
        AppliedInfo<T> InsertIfNotExists<T>(T poco, bool insertNulls, CqlQueryOptions queryOptions = null);
        
        /// <summary>
        /// Inserts the specified POCO in Cassandra, if not exists, with the provided execution profile.
        /// </summary>
        /// <param name="poco">The POCO instance</param>
        /// <param name="executionProfile">The execution profile to use when executing the request.</param>
        /// <param name="insertNulls">
        /// Determines if the query must be generated using <c>NULL</c> values for <c>null</c> POCO
        /// members. 
        /// <para>
        /// Use <c>false</c> if you don't want to consider <c>null</c> values for the INSERT
        /// operation (recommended).
        /// </para> 
        /// <para>
        /// Use <c>true</c> if you want to override all the values in the table,
        /// generating tombstones for null values.
        /// </para>
        /// </param>
        /// <param name="queryOptions">Optional query options</param>
        /// <returns></returns>
        AppliedInfo<T> InsertIfNotExists<T>(T poco, string executionProfile, bool insertNulls, CqlQueryOptions queryOptions = null);

        /// <summary>
        /// Inserts the specified POCO in Cassandra, if not exists.
        /// </summary>
        /// <param name="poco">The POCO instance</param>
        /// <param name="insertNulls">
        /// Determines if the query must be generated using <c>NULL</c> values for <c>null</c> POCO
        /// members. 
        /// <para>
        /// Use <c>false</c> if you don't want to consider <c>null</c> values for the INSERT
        /// operation (recommended).
        /// </para> 
        /// <para>
        /// Use <c>true</c> if you want to override all the values in the table,
        /// generating tombstones for null values.
        /// </para>
        /// </param>
        /// <param name="ttl">Time to live (in seconds) for the inserted values. If set, the inserted values are automatically removed
        /// from the database after the specified time.</param>
        /// <param name="queryOptions">Optional query options</param>
        /// <returns></returns>
        AppliedInfo<T> InsertIfNotExists<T>(T poco, bool insertNulls, int? ttl, CqlQueryOptions queryOptions = null);
        
        /// <summary>
        /// Inserts the specified POCO in Cassandra, if not exists, with the provided execution profile.
        /// </summary>
        /// <param name="poco">The POCO instance</param>
        /// <param name="executionProfile">The execution profile to use when executing the request.</param>
        /// <param name="insertNulls">
        /// Determines if the query must be generated using <c>NULL</c> values for <c>null</c> POCO
        /// members. 
        /// <para>
        /// Use <c>false</c> if you don't want to consider <c>null</c> values for the INSERT
        /// operation (recommended).
        /// </para> 
        /// <para>
        /// Use <c>true</c> if you want to override all the values in the table,
        /// generating tombstones for null values.
        /// </para>
        /// </param>
        /// <param name="ttl">Time to live (in seconds) for the inserted values. If set, the inserted values are automatically removed
        /// from the database after the specified time.</param>
        /// <param name="queryOptions">Optional query options</param>
        /// <returns></returns>
        AppliedInfo<T> InsertIfNotExists<T>(T poco, string executionProfile, bool insertNulls, int? ttl, CqlQueryOptions queryOptions = null);

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
        /// <remarks>
        /// To set the consistency level, timestamp and other batch options, use
        /// <see cref="ICqlBatch.WithOptions(System.Action{CqlQueryOptions})"/>. Individual options for each
        /// query within the batch will be ignored.
        /// </remarks>
        Task<AppliedInfo<T>> ExecuteConditionalAsync<T>(ICqlBatch batch);

        /// <summary>
        /// Executes a batch that contains a Lightweight transaction. 
        /// </summary>
        /// <para>
        /// Returns information whether it was applied or not. If it was not applied, it returns details of the existing values.
        /// </para>
        /// <remarks>
        /// To set the consistency level, timestamp and other batch options, use
        /// <see cref="ICqlBatch.WithOptions(System.Action{CqlQueryOptions})"/>. Individual options for each
        /// query within the batch will be ignored.
        /// </remarks>
        AppliedInfo<T> ExecuteConditional<T>(ICqlBatch batch);
    }
}
