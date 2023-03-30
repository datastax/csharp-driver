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
    /// Represents a QUERY or EXECUTE request that can be included in a batch
    /// </summary>
    internal interface IQueryRequest : IRequest
    {
        /// <summary>
        /// The paging state for the request
        /// </summary>
        byte[] PagingState { get; set; }

        /// <summary>
        /// Whether the skip_metadata flag is set for this request.
        /// </summary>
        bool SkipMetadata { get; }

        /// <summary>
        /// Method used by the batch to build each individual request
        /// </summary>
        void WriteToBatch(FrameWriter writer);
    }
}