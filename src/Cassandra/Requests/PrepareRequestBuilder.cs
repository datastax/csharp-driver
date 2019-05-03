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

namespace Cassandra.Requests
{
    internal class PrepareRequestBuilder : IPrepareRequestBuilder
    {
        internal IDictionary<string, byte[]> CustomPayload { get; private set; }

        internal string ExecutionProfileName { get; private set; } = Configuration.DefaultExecutionProfileName;

        internal string Query { get; private set; }

        /// <inheritdoc />
        public IPrepareRequestBuilder WithExecutionProfile(string executionProfileName)
        {
            if (string.IsNullOrWhiteSpace(executionProfileName))
            {
                throw new ArgumentNullException(nameof(executionProfileName));
            }

            ExecutionProfileName = executionProfileName;
            return this;
        }

        /// <inheritdoc />
        public IPrepareRequestBuilder WithQuery(string query)
        {
            Query = query;
            return this;
        }
        
        /// <inheritdoc />
        public IPrepareRequestBuilder WithCustomPayload(IDictionary<string, byte[]> customPayload)
        {
            CustomPayload = customPayload ?? throw new ArgumentNullException(nameof(customPayload));
            return this;
        }

        internal IPrepareRequest Build()
        {
            return new DefaultPrepareRequest(Query, CustomPayload, ExecutionProfileName);
        }
    }
}