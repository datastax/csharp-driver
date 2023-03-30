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

using System.Collections.Generic;
using System.Linq;
using Cassandra.DataStax.Graph;

namespace Cassandra.ExecutionProfiles
{
    /// <inheritdoc />
    internal class RequestOptionsMapper : IRequestOptionsMapper
    {
        /// <inheritdoc />
        public IReadOnlyDictionary<string, IRequestOptions> BuildRequestOptionsDictionary(
            IReadOnlyDictionary<string, IExecutionProfile> executionProfiles,
            Policies policies,
            SocketOptions socketOptions,
            ClientOptions clientOptions,
            QueryOptions queryOptions,
            GraphOptions graphOptions)
        {
            executionProfiles.TryGetValue(Configuration.DefaultExecutionProfileName, out var defaultProfile);
            var requestOptions =
                executionProfiles
                    .Where(kvp => kvp.Key != Configuration.DefaultExecutionProfileName)
                    .ToDictionary<KeyValuePair<string, IExecutionProfile>, string, IRequestOptions>(
                        kvp => kvp.Key,
                        kvp => new RequestOptions(kvp.Value, defaultProfile, policies, socketOptions, queryOptions, clientOptions, graphOptions));

            requestOptions.Add(
                Configuration.DefaultExecutionProfileName, 
                new RequestOptions(null, defaultProfile, policies, socketOptions, queryOptions, clientOptions, graphOptions));
            return requestOptions;
        }
    }
}