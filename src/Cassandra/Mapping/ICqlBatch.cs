using System;
using System.Collections.Generic;

namespace Cassandra.Mapping
{
    /// <summary>
    /// Represents a batch of CQL statements.  Use the write methods (Insert, Update, Delete, etc.) to add statements to the batch.
    /// </summary>
    public interface ICqlBatch : ICqlWriteClient
    {
        /// <summary>
        /// The statements in the batch.
        /// </summary>
        IEnumerable<Cql> Statements { get; }

        /// <summary>
        /// The type of batch to use.
        /// </summary>
        BatchType BatchType { get; }

        /// <summary>
        /// The execution options to use.
        /// </summary>
        CqlQueryOptions Options { get; }

        /// <summary>
        /// The execution profile to use.
        /// </summary>
        string ExecutionProfile { get; }

        /// <summary>
        /// Configures any individual option for this instance.
        /// </summary>
        ICqlBatch WithOptions(Action<CqlQueryOptions> action);

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

        /// <summary>
        /// Inserts the specified POCO in Cassandra if not exists.
        /// </summary>
        void InsertIfNotExists<T>(T poco, CqlQueryOptions queryOptions = null);
        
        /// <summary>
        /// Inserts the specified POCO in Cassandra if not exists.
        /// </summary>
        void InsertIfNotExists<T>(T poco, bool insertNulls, CqlQueryOptions queryOptions = null);
        
        /// <summary>
        /// Inserts the specified POCO in Cassandra if not exists.
        /// </summary>
        void InsertIfNotExists<T>(T poco, bool insertNulls, int? ttl, CqlQueryOptions queryOptions = null);
        
        /// <summary>
        /// Configures the execution profile for this instance.
        /// </summary>
        ICqlBatch WithExecutionProfile(string executionProfile);
    }
}