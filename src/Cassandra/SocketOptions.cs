//
//      Copyright (C) 2012-2014 DataStax Inc.
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
        public const int DefaultConnectTimeoutMillis = 5000;

        private int _connectTimeoutMillis = DefaultConnectTimeoutMillis;
        private bool _keepAlive = true;
        private int? _receiveBufferSize;
        private bool? _reuseAddress;
        private int? _sendBufferSize;
        private int? _soLinger;
        private bool? _tcpNoDelay;
        private bool _useStreamMode;

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
        /// false if the Socket uses the Nagle algorithm; otherwise, true. The default is false.
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
        /// false if the Socket uses the Nagle algorithm; otherwise, true. The default is false.
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
    }
}
