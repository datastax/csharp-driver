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
using System.Net;
using System.Net.Sockets;
using System.Threading.Tasks;
using Cassandra.Requests;
using Cassandra.Responses;
using Cassandra.Serialization;

namespace Cassandra.Connections
{
    /// <summary>
    /// Represents a TCP connection to a Cassandra Node
    /// </summary>
    internal interface IConnection : IDisposable
    {
        /// <summary>
        /// The event that represents a event RESPONSE from a Cassandra node
        /// </summary>
        event CassandraEventHandler CassandraEventResponse;

        /// <summary>
        /// Event raised when there is an error when executing the request to prevent idle disconnects
        /// </summary>
        event Action<Exception> OnIdleRequestException;

        /// <summary>
        /// Event that gets raised when a write has been completed. Testing purposes only.
        /// </summary>
        event Action WriteCompleted;

        /// <summary>
        /// Event that gets raised the connection is being closed.
        /// </summary>
        event Action<IConnection> Closing;

        ISerializer Serializer { get; }

        IFrameCompressor Compressor { get; set; }

        /// <summary>
        /// Remote EndPoint, i.e., endpoint to which this instance is connected.
        /// </summary>
        IConnectionEndPoint EndPoint { get; }

        IPEndPoint LocalAddress { get; }
        
        /// <summary>
        /// Length of the internal write queue (expensive operation!)
        /// </summary>
        int WriteQueueLength { get; }
        
        /// <summary>
        /// Length of the internal write queue (expensive operation!)
        /// </summary>
        int PendingOperationsMapLength { get; }

        /// <summary>
        /// Determines the amount of operations that are not finished.
        /// </summary>
        int InFlight { get; }
        
        /// <summary>
        /// Determines if there isn't any operations pending to be written or inflight.
        /// </summary>
        bool HasPendingOperations { get; }

        /// <summary>
        /// Gets the amount of operations that timed out and didn't get a response
        /// </summary>
        int TimedOutOperations { get; }

        /// <summary>
        /// Determine if the Connection has been explicitly disposed
        /// </summary>
        bool IsDisposed { get; }

        /// <summary>
        /// Gets the current keyspace.
        /// </summary>
        string Keyspace { get; }

        /// <summary>
        /// Gets the amount of concurrent requests depending on the protocol version
        /// </summary>
        int GetMaxConcurrentRequests(ISerializer serializer);

        ProtocolOptions Options { get; }
        
        /// <summary>
        /// Initializes the connection.
        /// </summary>
        /// <exception cref="SocketException">Throws a SocketException when the connection could not be established with the host</exception>
        /// <exception cref="AuthenticationException" />
        /// <exception cref="UnsupportedProtocolVersionException"></exception>
        Task<Response> Open();

        /// <summary>
        /// Sends a new request if possible. If it is not possible it queues it up.
        /// </summary>
        Task<Response> Send(IRequest request, int timeoutMillis);
        
        /// <summary>
        /// Sends a new request if possible and executes the callback when the response is parsed. If it is not possible it queues it up.
        /// </summary>
        OperationState Send(IRequest request, Action<IRequestError, Response> callback, int timeoutMillis);

        /// <summary>
        /// Sends a new request if possible with the default timeout. If it is not possible it queues it up.
        /// </summary>
        Task<Response> Send(IRequest request);

        /// <summary>
        /// Sends a new request if possible and executes the callback when the response is parsed with the default timeout. If it is not possible it queues it up.
        /// </summary>
        OperationState Send(IRequest request, Action<IRequestError, Response> callback);

        /// <summary>
        /// Sets the keyspace of the connection.
        /// If the keyspace is different from the current value, it sends a Query request to change it
        /// </summary>
        Task<bool> SetKeyspace(string value);
    }
}