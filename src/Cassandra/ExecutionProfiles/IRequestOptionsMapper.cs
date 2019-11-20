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
using Cassandra.DataStax.Graph;

namespace Cassandra.ExecutionProfiles
{
    /// <summary>
    /// Component that builds <see cref="IRequestOptions"/> instances from the provided <see cref="IExecutionProfile"/> instances.
    /// </summary>
    internal interface IRequestOptionsMapper
    {
        /// <summary>
        /// Converts execution profile instances to RequestOptions which handle the fallback logic
        /// therefore guaranteeing that the settings are non null.
        /// </summary>
        IReadOnlyDictionary<string, IRequestOptions> BuildRequestOptionsDictionary(
            IReadOnlyDictionary<string, IExecutionProfile> executionProfiles,
            Policies policies,
            SocketOptions socketOptions,
            ClientOptions clientOptions,
            QueryOptions queryOptions,
            GraphOptions graphOptions);
    }
}