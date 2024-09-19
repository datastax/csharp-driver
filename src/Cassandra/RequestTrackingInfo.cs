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

using System.Collections.Concurrent;
using Cassandra.Requests;

namespace Cassandra
{
    /// <summary>
    /// Class used to hold data that is passed to implementations of <see cref="IRequestTracker"/>.
    /// Either <see cref="Statement"/> or <see cref="PrepareRequest"/> will be set, never both.
    /// </summary>
    public sealed class RequestTrackingInfo
    {
        internal RequestTrackingInfo(IStatement statement)
        {
            Statement = statement;
        }

        internal RequestTrackingInfo(InternalPrepareRequest prepareRequest)
        {
            PrepareRequest = new PrepareRequest(prepareRequest.Query, prepareRequest.Keyspace);
        }

        public ConcurrentDictionary<string, object> Items { get; } = new ConcurrentDictionary<string, object>();

        /// <summary>
        /// If this request is associated with a <see cref="IStatement"/> object, then this property is set.
        /// </summary>
        public IStatement Statement { get; }

        /// <summary>
        /// If this request is a PREPARE request, then this property is set.
        /// </summary>
        public PrepareRequest PrepareRequest { get; }
        
    }
}
