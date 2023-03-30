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

namespace Cassandra
{
    /// <summary>
    ///  Options to configure low-level socket options for the connections kept to the
    ///  Cassandra hosts.
    /// </summary>
    public class SocketOptions
    {
        /// <summary>
        /// Default value for <see cref="ConnectTimeoutMillis"/>, 5000ms.
        /// </summary>
        public const int DefaultConnectTimeoutMillis = 5000;
        /// <summary>
        /// Default value for <see cref="ReadTimeoutMillis"/>, 12000ms.
        /// </summary>
        public const int DefaultReadTimeoutMillis = 12000;
        /// <summary>
        /// Default value for <see cref="DefunctReadTimeoutThreshold"/>, 64.
        /// </summary>
        internal const int DefaultDefunctReadTimeoutThreshold = 64;
        private int _connectTimeoutMillis = DefaultConnectTimeoutMillis;
        private bool _keepAlive = true;
        private int? _receiveBufferSize;
        private bool? _reuseAddress;
        private int? _sendBufferSize;
        private int? _soLinger;
        private bool _tcpNoDelay = true;
        private bool _useStreamMode;
        private int _readTimeoutMillis = DefaultReadTimeoutMillis;
        private int _defunctReadTimeoutThreshold = DefaultDefunctReadTimeoutThreshold;
        private int _metadataAbortTimeout =  5 * 60000;

        /// <summary>
        /// Gets the number of milliseconds to wait for the socket to connect
        /// </summary>
        public int ConnectTimeoutMillis
        {
            get { return _connectTimeoutMillis; }
        }

        /// <summary>
        /// Gets if TCP keep-alive must be used 
        /// </summary>
        public bool? KeepAlive
        {
            get { return _keepAlive; }
        }

        public bool? ReuseAddress
        {
            get { return _reuseAddress; }
        }

        /// <summary>
        /// Gets the number of seconds to remain open after the Socket.Close() is called.
        /// </summary>
        public int? SoLinger
        {
            get { return _soLinger; }
        }

        /// <summary>
        /// Gets a Boolean value that specifies whether the stream Socket is using the Nagle algorithm.
        /// When set to <c>false</c>, the Nagle algorithm is enabled; when set to <c>true</c> the Nagle algorithm is disabled (no delay). The default is <c>true</c>.
        /// </summary>
        public bool? TcpNoDelay
        {
            get { return _tcpNoDelay; }
        }

        /// <summary>
        /// Gets the size of the buffer used by the socket to receive
        /// </summary>
        public int? ReceiveBufferSize
        {
            get { return _receiveBufferSize; }
        }

        /// <summary>
        /// Gets the size of the buffer used by the socket to send
        /// </summary>
        public int? SendBufferSize
        {
            get { return _sendBufferSize; }
        }

        /// <summary>
        /// Determines if the driver should use either .NET NetworkStream interface (true) or SocketEventArgs interface (false, default)
        /// to handle the reading and writing
        /// </summary>
        public bool UseStreamMode
        {
            get { return _useStreamMode; }
        }

        /// <summary>
        /// The per-host read timeout in milliseconds.
        /// <para>
        /// This defines how long the driver will wait for a given Cassandra node to answer a query.
        /// </para>
        /// Please note that this is not the maximum time a call to <see cref="Session.Execute(string)"/> may block; this is the maximum time that call will wait for one particular Cassandra host, but other hosts will be tried if one of them timeout. In other words, a <see cref="Session.Execute(string)"/> call may theoretically wait up to ReadTimeoutMillis * {number_of_cassandra_hosts} (though the total number of hosts tried for a given query also depends on the LoadBalancingPolicy in use).
        /// Also note that for efficiency reasons, this read timeout is approximate, it may fire up to late. It is not meant to be used for precise timeout, but rather as a protection against misbehaving Cassandra nodes.
        /// </summary>
        public int ReadTimeoutMillis
        {
            get { return _readTimeoutMillis; }
        }

        internal int MetadataAbortTimeout => _metadataAbortTimeout;

        /// <summary>
        /// Gets the amount of requests that simultaneously have to timeout before closing the connection.
        /// </summary>
        public int DefunctReadTimeoutThreshold
        {
            get { return _defunctReadTimeoutThreshold; }
        }

        /// <summary>
        /// Sets the number of milliseconds to wait for the socket to connect
        /// </summary>
        public SocketOptions SetConnectTimeoutMillis(int connectTimeoutMillis)
        {
            _connectTimeoutMillis = connectTimeoutMillis;
            return this;
        }

        /// <summary>
        /// Sets if TCP keep-alive must be used 
        /// </summary>
        public SocketOptions SetKeepAlive(bool keepAlive)
        {
            _keepAlive = keepAlive;
            return this;
        }

        public SocketOptions SetReuseAddress(bool reuseAddress)
        {
            _reuseAddress = reuseAddress;
            return this;
        }

        /// <summary>
        /// Sets the number of seconds to remain open after the Socket.Close() is called.
        /// </summary>
        public SocketOptions SetSoLinger(int soLinger)
        {
            _soLinger = soLinger;
            return this;
        }

        /// <summary>
        /// Set a Boolean value that specifies whether the stream Socket is using the Nagle algorithm.
        /// Use <c>false</c> to enable Nagle algorithm; use <c>true</c> to disable Nagle algorithm (no delay). The default is <c>true</c>.
        /// </summary>
        public SocketOptions SetTcpNoDelay(bool tcpNoDelay)
        {
            _tcpNoDelay = tcpNoDelay;
            return this;
        }

        /// <summary>
        /// Sets the size of the buffer used by the socket to receive
        /// </summary>
        public SocketOptions SetReceiveBufferSize(int receiveBufferSize)
        {
            _receiveBufferSize = receiveBufferSize;
            return this;
        }

        /// <summary>
        /// Sets the size of the buffer used by the socket to send
        /// </summary>
        public SocketOptions SetSendBufferSize(int sendBufferSize)
        {
            _sendBufferSize = sendBufferSize;
            return this;
        }

        /// <summary>
        /// Sets if the driver should use either .NET NetworkStream (true) interface or SocketEventArgs interface (false, default)
        /// to handle the reading and writing
        /// </summary>
        public SocketOptions SetStreamMode(bool useStreamMode)
        {
            _useStreamMode = useStreamMode;
            return this;
        }

        /// <summary>
        /// Sets the per-host read timeout in milliseconds.
        /// <para>When setting this value, keep in mind the following:</para>
        /// <para>- the timeout settings used on the Cassandra side (*_request_timeout_in_ms in cassandra.yaml) should be taken into account when picking a value for this read timeout. In particular, if this read timeout option is lower than Cassandra's timeout, the driver might assume that the host is not responsive and mark it down.</para>
        /// <para>- the read timeout is only approximate and only control the timeout to one Cassandra host, not the full query (see <see cref="ReadTimeoutMillis"/> for more details).</para>
        /// Setting a value of 0 disables client read timeouts.
        /// </summary>
        public SocketOptions SetReadTimeoutMillis(int milliseconds)
        {
            _readTimeoutMillis = milliseconds;
            return this;
        }

        /// <summary>
        /// Determines the amount of requests that simultaneously have to timeout before closing the connection.
        /// </summary>
        public SocketOptions SetDefunctReadTimeoutThreshold(int amountOfTimeouts)
        {
            _defunctReadTimeoutThreshold = amountOfTimeouts;
            return this;
        }

        internal SocketOptions SetMetadataAbortTimeout(int metadataAbortTimeout)
        {
            _metadataAbortTimeout = metadataAbortTimeout;
            return this;
        }
    }
}
