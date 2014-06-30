//
//      Copyright (C) 2012 DataStax Inc.
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
        private bool? _keepAlive = true;
        private int? _receiveBufferSize;
        private bool? _reuseAddress;
        private int? _sendBufferSize;
        private int? _soLinger;
        private bool? _tcpNoDelay;

        public int ConnectTimeoutMillis
        {
            get { return _connectTimeoutMillis; }
        }

        public bool? KeepAlive
        {
            get { return _keepAlive; }
        }

        public bool? ReuseAddress
        {
            get { return _reuseAddress; }
        }

        public int? SoLinger
        {
            get { return _soLinger; }
        }

        public bool? TcpNoDelay
        {
            get { return _tcpNoDelay; }
        }

        public int? ReceiveBufferSize
        {
            get { return _receiveBufferSize; }
        }

        public int? SendBufferSize
        {
            get { return _sendBufferSize; }
        }

        public SocketOptions SetConnectTimeoutMillis(int connectTimeoutMillis)
        {
            _connectTimeoutMillis = connectTimeoutMillis;
            return this;
        }

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

        public SocketOptions SetSoLinger(int soLinger)
        {
            _soLinger = soLinger;
            return this;
        }

        public SocketOptions SetTcpNoDelay(bool tcpNoDelay)
        {
            _tcpNoDelay = tcpNoDelay;
            return this;
        }

        public SocketOptions SetReceiveBufferSize(int receiveBufferSize)
        {
            _receiveBufferSize = receiveBufferSize;
            return this;
        }

        public SocketOptions SetSendBufferSize(int sendBufferSize)
        {
            _sendBufferSize = sendBufferSize;
            return this;
        }
    }
}