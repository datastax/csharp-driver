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

namespace Cassandra.Requests
{
    /// <summary>
    /// Represents an CQL Request (BATCH, EXECUTE or QUERY)
    /// </summary>
    internal interface ICqlRequest : IRequest
    {
        /// <summary>
        /// Gets or sets the Consistency for the Request.
        /// It defaults to the one provided by the Statement but it can be changed by the retry policy.
        /// </summary>
        ConsistencyLevel Consistency { get; set; }
    }
}
