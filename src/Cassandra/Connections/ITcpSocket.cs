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
using Microsoft.IO;

namespace Cassandra.Connections
{
    internal interface ITcpSocket : IDisposable
    {
        IConnectionEndPoint EndPoint { get; }

        SocketOptions Options { get; }

        SSLOptions SSLOptions { get; set; }

        /// <summary>
        /// Event that gets fired when new data is received.
        /// </summary>
        event Action<byte[], int> Read;

        /// <summary>
        /// Event that gets fired when a write async request have been completed.
        /// </summary>
        event Action WriteCompleted;

        /// <summary>
        /// Event that is fired when the host is closing the connection.
        /// </summary>
        event Action Closing;

        event Action<Exception, SocketError?> Error;

        /// <summary>
        /// Get this socket's local address.
        /// </summary>
        /// <returns>The socket's local address.</returns>
        IPEndPoint GetLocalIpEndPoint();

        /// <summary>
        /// Initializes the socket options
        /// </summary>
        void Init();

        /// <summary>
        /// Connects asynchronously to the host and starts reading
        /// </summary>
        /// <exception cref="SocketException">Throws a SocketException when the connection could not be established with the host</exception>
        Task<bool> Connect();

        /// <summary>
        /// Sends data asynchronously
        /// </summary>
        void Write(RecyclableMemoryStream stream, Action onBufferFlush);

        void Kill();
    }
}