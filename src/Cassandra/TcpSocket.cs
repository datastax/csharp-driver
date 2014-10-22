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

﻿using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Net;
using System.Net.Security;
using System.Net.Sockets;
using System.Security.Cryptography.X509Certificates;
using System.Text;

namespace Cassandra
{
    /// <summary>
    /// Represents a Tcp connection to a host.
    /// It emits Read and WriteCompleted events when data is received.
    /// Similar to Netty's Channel or Node.js's net.Socket
    /// It handles TLS validation and encryption when required.
    /// </summary>
    internal class TcpSocket
    {
        private static Logger _logger = new Logger(typeof(TcpSocket));
        private Socket _socket;
        private SocketAsyncEventArgs _receiveSocketEvent;
        private SocketAsyncEventArgs _sendSocketEvent;
        private Stream _socketStream;
        private byte[] _receiveBuffer;
        private volatile bool _isClosing = false;
        
        public IPEndPoint IPEndPoint { get; protected set; }

        public SocketOptions Options { get; protected set; }

        public SSLOptions SSLOptions { get; set; }

        /// <summary>
        /// Event that gets fired when new data is received.
        /// </summary>
        public event Action<byte[], int> Read;

        /// <summary>
        /// Event that gets fired when a write async request have been completed.
        /// </summary>
        public event Action WriteCompleted;

        /// <summary>
        /// Event that is fired when the host is closing the connection.
        /// </summary>
        public event Action Closing;

        public event Action<Exception, SocketError?> Error; 

        /// <summary>
        /// Creates a new instance of TcpSocket using the endpoint and options provided.
        /// </summary>
        public TcpSocket(IPEndPoint ipEndPoint, SocketOptions options, SSLOptions sslOptions)
        {
            this.IPEndPoint = ipEndPoint;
            this.Options = options;
            this.SSLOptions = sslOptions;
        }

        /// <summary>
        /// Initializes the socket options
        /// </summary>
        public void Init()
        {
            _socket = new Socket(AddressFamily.InterNetwork, SocketType.Stream, ProtocolType.Tcp);
            if (Options.KeepAlive != null)
            {
                _socket.SetSocketOption(SocketOptionLevel.Socket, SocketOptionName.KeepAlive, Options.KeepAlive.Value);
            }

            _socket.SendTimeout = Options.ConnectTimeoutMillis;
            if (Options.SoLinger != null)
            {
                _socket.SetSocketOption(SocketOptionLevel.Socket, SocketOptionName.Linger, new LingerOption(true, Options.SoLinger.Value));
            }
            if (Options.ReceiveBufferSize != null)
            {
                _socket.ReceiveBufferSize = Options.ReceiveBufferSize.Value;
            }
            if (Options.SendBufferSize != null)
            {
                _socket.ReceiveBufferSize = Options.SendBufferSize.Value;
            }
            if (Options.TcpNoDelay != null)
            {
                _socket.NoDelay = Options.TcpNoDelay.Value;
            }
            _receiveBuffer = new byte[_socket.ReceiveBufferSize];
        }

        /// <summary>
        /// Connects synchronously to the host and starts reading
        /// </summary>
        /// <exception cref="SocketException">Throws a SocketException when the connection could not be established with the host</exception>
        public void Connect()
        {
            var connectResult = _socket.BeginConnect(IPEndPoint, null, null);
            var connectSignaled = connectResult.AsyncWaitHandle.WaitOne(Options.ConnectTimeoutMillis);

            if (!connectSignaled)
            {
                //It timed out: Close the socket and throw the exception
                _socket.Close();
                throw new SocketException((int)SocketError.TimedOut);
            }
            //End the connect process
            //It will throw exceptions in case there was a problem
            _socket.EndConnect(connectResult);

            //Prepare read and write
            //There are 2 modes: using SocketAsyncEventArgs (most performant) and Stream mode with APM methods
            if (SSLOptions == null && !Options.UseStreamMode)
            {
                _logger.Verbose("Socket connected, start reading using SocketEventArgs interface");
                //using SocketAsyncEventArgs
                _receiveSocketEvent = new SocketAsyncEventArgs();
                _receiveSocketEvent.SetBuffer(_receiveBuffer, 0, _receiveBuffer.Length);
                _receiveSocketEvent.Completed += OnReceiveCompleted;
                _sendSocketEvent = new SocketAsyncEventArgs();
                _sendSocketEvent.Completed += OnSendCompleted;
            }
            else
            {
                _logger.Verbose("Socket connected, start reading using Stream interface");
                //Stream mode: not the most performant but it has ssl support
                _socketStream = new NetworkStream(_socket);
                if (SSLOptions != null)
                {
                    string targetHost = targetHost = IPEndPoint.Address.ToString();
                    try
                    {
                        targetHost = SSLOptions.HostNameResolver(IPEndPoint.Address);
                    }
                    catch (Exception ex)
                    {
                        _logger.Error(String.Format("SSL connection: Can not resolve host name for address {0}. Using the IP address instead of the host name. This may cause RemoteCertificateNameMismatch error during Cassandra host authentication. Note that the Cassandra node SSL certificate's CN(Common Name) must match the Cassandra node hostname.", targetHost), ex);
                    }
                    _socketStream = new SslStream(_socketStream, false, SSLOptions.RemoteCertValidationCallback, null);
                    var sslAuthResult = (_socketStream as SslStream).BeginAuthenticateAsClient(targetHost, SSLOptions.CertificateCollection, SSLOptions.SslProtocol, SSLOptions.CheckCertificateRevocation, null, null);
                    var sslAuthSignaled = sslAuthResult.AsyncWaitHandle.WaitOne(Options.ConnectTimeoutMillis);
                    if (!sslAuthSignaled)
                    {
                        //It timed out: Close the socket and throw the exception
                        _socket.Close();
                        throw new SocketException((int)SocketError.TimedOut);
                    }
                    (_socketStream as SslStream).EndAuthenticateAsClient(sslAuthResult);
                }
            }

            ReceiveAsync();
        }

        /// <summary>
        /// Begins an asynchronous request to receive data from a connected Socket object.
        /// It handles the exceptions in case there is one.
        /// </summary>
        protected virtual void ReceiveAsync()
        {
            //Receive the next bytes
            if (_receiveSocketEvent != null)
            {
                bool willRaiseEvent = true;
                try
                {
                    willRaiseEvent = _socket.ReceiveAsync(_receiveSocketEvent);
                }
                catch (Exception ex)
                {
                    OnError(ex);
                }
                if (!willRaiseEvent)
                {
                    OnReceiveCompleted(this, _receiveSocketEvent);
                }
            }
            else
            {
                //Stream mode
                _socketStream.BeginRead(_receiveBuffer, 0, _receiveBuffer.Length, new AsyncCallback(OnReceiveStreamCallback), null);
            }
        }

        protected virtual void OnError(Exception ex, SocketError? socketError = null)
        {
            if (Error != null)
            {
                Error(ex, socketError);
            }
        }

        /// <summary>
        /// Handles the receive completed event
        /// </summary>
        protected void OnReceiveCompleted(object sender, SocketAsyncEventArgs e)
        {
            if (e.SocketError != SocketError.Success)
            {
                //There was a socket error or the connection is being closed.
                OnError(null, e.SocketError);
                return;
            }
            else if (e.BytesTransferred == 0)
            {
                OnClosing();
                return;
            }

            //Emit event
            if (Read != null)
            {
                Read(e.Buffer, e.BytesTransferred);
            }

            ReceiveAsync();
        }

        /// <summary>
        /// Handles the callback for BeginRead on Stream mode
        /// </summary>
        protected void OnReceiveStreamCallback(IAsyncResult ar)
        {
            try
            {
                var bytesRead = _socketStream.EndRead(ar);
                if (bytesRead == 0)
                {
                    OnClosing();
                    return;
                }
                //Emit event
                if (Read != null)
                {
                    Read(_receiveBuffer, bytesRead);
                }
                ReceiveAsync();
            }
            catch (Exception ex)
            {
                if (ex is IOException && ex.InnerException is SocketException)
                {
                    OnError((SocketException)ex.InnerException);
                }
                else
                {
                    OnError(ex);
                }
            }
        }

        /// <summary>
        /// Handles the send completed event
        /// </summary>
        protected void OnSendCompleted(object sender, SocketAsyncEventArgs e)
        {
            if (e.SocketError != SocketError.Success)
            {
                OnError(null, e.SocketError);
            }
            if (WriteCompleted != null)
            {
                WriteCompleted();
            }
        }

        /// <summary>
        /// Handles the callback for BeginWrite on Stream mode
        /// </summary>
        protected void OnSendStreamCallback(IAsyncResult ar)
        {
            try
            {
                _socketStream.EndWrite(ar);
            }
            catch (Exception ex)
            {
                if (ex is IOException && ex.InnerException is SocketException)
                {
                    OnError((SocketException)ex.InnerException);
                }
                else
                {
                    OnError(ex);
                }
            }
            if (WriteCompleted != null)
            {
                WriteCompleted();
            }
        }

        protected virtual void OnClosing()
        {
            _isClosing = true;
            if (Closing != null)
            {
                Closing();
            }
        }

        /// <summary>
        /// Sends data asynchronously
        /// </summary>
        public virtual void Write(Stream stream)
        {
            if (_isClosing)
            {
                OnError(new SocketException((int)SocketError.Shutdown));
            }
            //This can result in OOM
            //A neat improvement would be to write this sync in small buffers when buffer.length > X
            var buffer = Utils.ReadAllBytes(stream, 0);
            if (_sendSocketEvent != null)
            {
                _sendSocketEvent.SetBuffer(buffer, 0, buffer.Length);
                bool willRaiseEvent = false;
                try
                {
                    willRaiseEvent = _socket.SendAsync(_sendSocketEvent);
                }
                catch (Exception ex)
                {
                    OnError(ex);
                }
                if (!willRaiseEvent)
                {
                    OnSendCompleted(this, _sendSocketEvent);
                }
            }
            else
            {
                _socketStream.BeginWrite(buffer, 0, buffer.Length, new AsyncCallback(OnSendStreamCallback), null);
            }
        }

        internal void Kill()
        {
            _socket.Shutdown(SocketShutdown.Send);
        }

        public void Dispose()
        {
            try
            {
                if (_socket == null)
                {
                    return;
                }
                //Try to close it.
                //Some operations could make the socket to dispose itself
                _socket.Shutdown(SocketShutdown.Both);
                _socket.Close();
            }
            catch
            {
                //We should not mind if the socket shutdown or close methods throw an exception
            }
        }
    }
}
