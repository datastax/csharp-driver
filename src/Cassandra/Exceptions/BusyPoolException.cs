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

using System.Net;
using Cassandra.Connections;

namespace Cassandra
{
    /// <summary>
    /// Represents a client-side error indicating that all connections to a certain host have reached
    /// the maximum amount of in-flight requests supported.
    /// </summary>
    public class BusyPoolException : DriverException
    {
        /// <summary>
        /// Gets the host address.
        /// </summary>
        public IPEndPoint Address { get; }

        /// <summary>
        /// Gets the maximum amount of requests per connection.
        /// </summary>
        public int MaxRequestsPerConnection { get; }

        /// <summary>
        /// Gets the size of the pool.
        /// </summary>
        public int ConnectionLength { get; }

        /// <summary>
        /// Creates a new instance of <see cref="BusyPoolException"/>.
        /// </summary>
        public BusyPoolException(IPEndPoint address, int maxRequestsPerConnection, int connectionLength)
            : base(BusyPoolException.GetMessage(address, maxRequestsPerConnection, connectionLength))
        {
            Address = address;
            MaxRequestsPerConnection = maxRequestsPerConnection;
            ConnectionLength = connectionLength;
        }
        
        private static string GetMessage(IPEndPoint address, int maxRequestsPerConnection, int connectionLength)
        {
            return $"All connections to host {address} are busy, {maxRequestsPerConnection} requests " +
                   $"are in-flight on {(connectionLength > 0 ? "each " : "")}{connectionLength} connection(s)";
        }
        
        private static string GetMessage(IConnectionEndPoint endPoint, int maxRequestsPerConnection, int connectionLength)
        {
            return $"All connections to host {endPoint.EndpointFriendlyName} are busy, {maxRequestsPerConnection} requests " +
                   $"are in-flight on {(connectionLength > 0 ? "each " : "")}{connectionLength} connection(s)";
        }
    }
}