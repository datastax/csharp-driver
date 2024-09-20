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

namespace Cassandra
{
    /// <summary>
    /// Class used to hold data that is passed to implementations of <see cref="IRequestTracker"/>.
    /// </summary>
    public class NodeRequestInfo : IEquatable<NodeRequestInfo>
    {
        /// <summary>
        /// Host that this node request is being sent to.
        /// </summary>
        public Host Host { get; }

        /// <summary>
        /// Each node request has a unique ID so that <see cref="IRequestTracker"/> implementations can maintain context per node request.
        /// </summary>
        public Guid ExecutionId { get; }

        /// <summary>
        /// Note that this can be different to the parent's <see cref="SessionRequestInfo.PrepareRequest"/>.
        /// An example case where this can happen is if the session request is a <see cref="BoundStatement"/> (so <see cref="SessionRequestInfo.PrepareRequest"/> is null)
        /// and this node request is a PREPARE because the server replied with an UNPREPARED response so the driver has to re-prepare the statement before retrying the <see cref="BoundStatement"/>.
        /// </summary>
        public PrepareRequest PrepareRequest { get; }

        internal NodeRequestInfo(Host host, PrepareRequest prepareRequest)
        {
            Host = host;
            ExecutionId = Guid.NewGuid();
            PrepareRequest = prepareRequest;
        }

        public bool Equals(NodeRequestInfo other)
        {
            return other != null && other.ExecutionId.Equals(ExecutionId);
        }

        public static bool operator ==(NodeRequestInfo a, NodeRequestInfo b)
        {
            if (a == null)
            {
                return b == null;
            }

            return a.Equals(b);
        }

        public static bool operator !=(NodeRequestInfo a, NodeRequestInfo b)
        {
            return !(a == b);
        }

        public override bool Equals(object obj)
        {
            if (!(obj is NodeRequestInfo info))
            {
                return false;
            }
            return Equals(info);
        }

        public override int GetHashCode()
        {
            unchecked
            {
                return ((Host != null ? Host.GetHashCode() : 0) * 397) ^ ExecutionId.GetHashCode();
            }
        }
    }
}
