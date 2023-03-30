//
//      Copyright (C) DataStax Inc.
//
//   Licensed under the Apache License, Version 2.0 (the "License");
//   you may not use this file except in compliance with the License.
//   You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
//   Unless required by applicable law or agreed to in writing, software
//   distributed under the License is distributed on an "AS IS" BASIS,
//   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//   See the License for the specific language governing permissions and
//   limitations under the License.
//

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
    }
}