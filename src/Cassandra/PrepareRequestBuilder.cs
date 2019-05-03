// 
//       Copyright (C) 2019 DataStax Inc.
// 
//    Licensed under the Apache License, Version 2.0 (the "License");
//    you may not use this file except in compliance with the License.
//    You may obtain a copy of the License at
// 
//       http://www.apache.org/licenses/LICENSE-2.0
// 
//    Unless required by applicable law or agreed to in writing, software
//    distributed under the License is distributed on an "AS IS" BASIS,
//    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//    See the License for the specific language governing permissions and
//    limitations under the License.
// 

using System;
using System.Collections.Generic;
using Cassandra.Requests;

namespace Cassandra
{
    /// <inheritdoc />
    public sealed class PrepareRequestBuilder : IPrepareRequestBuilder
    {
        private IDictionary<string, byte[]> CustomPayload { get; set; }

        private string ExecutionProfileName { get; set; } = Configuration.DefaultExecutionProfileName;

        private string Query { get; }

        private PrepareRequestBuilder(string query)
        {
            Query = query;
        }

        /// <inheritdoc />
        IPrepareRequestBuilder IPrepareRequestBuilder.WithExecutionProfile(string executionProfileName)
        {
            if (string.IsNullOrWhiteSpace(executionProfileName))
            {
                throw new ArgumentNullException(nameof(executionProfileName));
            }

            ExecutionProfileName = executionProfileName;
            return this;
        }
        
        /// <inheritdoc />
        IPrepareRequestBuilder IPrepareRequestBuilder.WithCustomPayload(IDictionary<string, byte[]> customPayload)
        {
            CustomPayload = customPayload ?? throw new ArgumentNullException(nameof(customPayload));
            return this;
        }

        /// <summary>
        /// Creates a prepare request builder from a cql query.
        /// </summary>
        public static IPrepareRequestBuilder FromQuery(string query)
        {
            return new PrepareRequestBuilder(query);
        }
        
        /// <inheritdoc />
        IPrepareRequest IPrepareRequestBuilder.Build()
        {
            return new DefaultPrepareRequest(Query, CustomPayload, ExecutionProfileName);
        }
    }
}