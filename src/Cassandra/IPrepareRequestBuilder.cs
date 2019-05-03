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

using System.Collections.Generic;

namespace Cassandra
{
    /// <summary>
    /// Builder that allows the client application to specify the query and options related to a Prepare request.
    /// See <see cref="ISession.PrepareAsync(IPrepareRequest)"/>;
    /// </summary>
    public interface IPrepareRequestBuilder
    {
        /// <summary>
        /// Specifies the custom payload to use in the Prepare request.
        /// </summary>
        IPrepareRequestBuilder WithCustomPayload(IDictionary<string, byte[]> customPayload);

        /// <summary>
        /// Specifies the execution profile to use in the Prepare request.
        /// </summary>
        IPrepareRequestBuilder WithExecutionProfile(string executionProfileName);

        /// <summary>
        /// Builds a prepare request with the options and query provided to this builder.
        /// </summary>
        IPrepareRequest Build();
    }
}