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
using System.IO;
using System.Net;
using System.Net.Security;
using System.Net.Sockets;
﻿using System.Threading;
﻿using System.Threading.Tasks;
﻿using Cassandra.Tasks;

namespace Cassandra
{
    /// <summary>
    /// Represents a Tcp connection to a host.
    /// It emits Read and WriteCompleted events when data is received.
    /// Similar to Netty's Channel or Node.js's net.Socket
    /// It handles TLS validation and encryption when required.
    /// </summary>
    internal class TcpSocket: IDisposable
    {
        private static Logger _logger = new Logger(typeof(TcpSocket));
        private Socket _socket;
        private SocketAsyncEventArgs _receiveSocketEvent;
        private SocketAsyncEventArgs _sendSocketEvent;
        private Stream _socketStream;
        private byte[] _receiveBuffer;
        private volatile bool _isClosing;
        
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
            IPEndPoint = ipEndPoint;
            Options = options;
            SSLOptions = sslOptions;
        }

        /// <summary>
        /// Initializes the socket options
        /// </summary>
        public void Init()
        {
            _socket = new Socket(IPEndPoint.AddressFamily, SocketType.Stream, ProtocolType.Tcp);
            _socket.SendTimeout = Options.ConnectTimeoutMillis;
            if (Options.KeepAlive != null)
            {
                _socket.SetSocketOption(SocketOptionLevel.Socket, SocketOptionName.KeepAlive, Options.KeepAlive.Value);
            }
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
                _socket.SendBufferSize = Options.SendBufferSize.Value;
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
        public Task<bool> Connect()
        {
            var tcs = new TaskCompletionSource<bool>();
            var socketConnectTask = tcs.Task;
            var eventArgs = new SocketAsyncEventArgs
            {
                RemoteEndPoint = IPEndPoint
            };
            var timer = new Timer(state =>
            {
                tcs.TrySetException(new SocketException((int)SocketError.TimedOut));
                eventArgs.Dispose();
                ((Timer)state).Dispose();
            });

            eventArgs.Completed += (sender, e) =>
            {
                if (e.SocketError != SocketError.Success)
                {
                    tcs.TrySetException(new SocketException((int)e.SocketError));
                    return;
                }
                tcs.TrySetResult(true);
                e.Dispose();
                // ReSharper disable once PossibleNullReferenceException
                timer.Dispose();
            };

            try
            {
                _socket.ConnectAsync(eventArgs);
            }
            catch (Exception ex)
            {
                return TaskHelper.FromException<bool>(ex);
            }

            try
            {
                timer.Change(Options.ConnectTimeoutMillis, Timeout.Infinite);
            }
            catch (ObjectDisposedException)
            {
                //It could be disposed by now
            }
            //Prepare read and write
            //There are 2 modes: using SocketAsyncEventArgs (most performant) and Stream mode with APM methods
            if (SSLOptions == null && !Options.UseStreamMode)
            {
                return socketConnectTask.ContinueSync(_ =>
                {
                    _logger.Verbose("Socket connected, start reading using SocketEventArgs interface");
                    //using SocketAsyncEventArgs
                    _receiveSocketEvent = new SocketAsyncEventArgs();
                    _receiveSocketEvent.SetBuffer(_receiveBuffer, 0, _receiveBuffer.Length);
                    _receiveSocketEvent.Completed += OnReceiveCompleted;
                    _sendSocketEvent = new SocketAsyncEventArgs();
                    _sendSocketEvent.Completed += OnSendCompleted;
                    ReceiveAsync();
                    return true;
                });
            }
            if (SSLOptions == null)
            {
                return socketConnectTask.ContinueSync(_ =>
                {
                    _logger.Verbose("Socket connected, start reading using Stream interface");
                    //Stream mode: not the most performant but it is a choice
                    _socketStream = new NetworkStream(_socket);
                    ReceiveAsync();
                    return true;
                });
            }
            return socketConnectTask.Then(_ => ConnectSsl());
        }

        private Task<bool> ConnectSsl()
        {
            _logger.Verbose("Socket connected, starting SSL client authentication");
            //Stream mode: not the most performant but it has ssl support
            var targetHost = IPEndPoint.Address.ToString();
            //HostNameResolver is a sync operation but it can block
            //Use another thread
            return Task.Factory.StartNew(() =>
            {
                try
                {
                    targetHost = SSLOptions.HostNameResolver(IPEndPoint.Address);
                }
                catch (Exception ex)
                {
                    _logger.Error(String.Format("SSL connection: Can not resolve host name for address {0}. Using the IP address instead of the host name. This may cause RemoteCertificateNameMismatch error during Cassandra host authentication. Note that the Cassandra node SSL certificate's CN(Common Name) must match the Cassandra node hostname.", targetHost), ex);
                }
                return true;
            }).Then(_ =>
            {
                _logger.Verbose("Starting SSL authentication");
                var tcs = new TaskCompletionSource<bool>();
                var sslStream = new SslStream(new NetworkStream(_socket), false, SSLOptions.RemoteCertValidationCallback, null);
                _socketStream = sslStream;
                sslStream.BeginAuthenticateAsClient(targetHost, SSLOptions.CertificateCollection, SSLOptions.SslProtocol, SSLOptions.CheckCertificateRevocation, 
                    sslAsyncResult =>
                    {
                        try
                        {
                            sslStream.EndAuthenticateAsClient(sslAsyncResult);
                            tcs.TrySetResult(true);
                        }
                        catch (Exception ex)
                        {
                            tcs.TrySetException(ex);
                        }
                    }, null);
                return tcs.Task;
            }).ContinueSync(_ =>
            {
                _logger.Verbose("SSL authentication successful");
                ReceiveAsync();
                return true;
            });
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
                var willRaiseEvent = true;
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
                _socketStream.BeginRead(_receiveBuffer, 0, _receiveBuffer.Length, OnReceiveStreamCallback, null);
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
            if (e.BytesTransferred == 0)
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
            if (_receiveSocketEvent != null)
            {
                //It is safe to call SocketAsyncEventArgs.Dispose() more than once
                _sendSocketEvent.Dispose();
                _receiveSocketEvent.Dispose();
            }
            else if (_socketStream != null)
            {
                _socketStream.Dispose();
            }
            //dereference to make the byte array GC-able as soon as possible
            _receiveBuffer = null;
        }

        /// <summary>
        /// Sends data asynchronously
        /// </summary>
        public virtual void Write(byte[] buffer)
        {
            if (_isClosing)
            {
                OnError(new SocketException((int)SocketError.Shutdown));
                return;
            }
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
                _socketStream.BeginWrite(buffer, 0, buffer.Length, OnSendStreamCallback, null);
            }
        }

        internal void Kill()
        {
            _socket.Shutdown(SocketShutdown.Send);
        }

        public void Dispose()
        {
            if (_socket == null)
            {
                return;
            }
            _isClosing = true;
            try
            {
                //Try to close it.
                //Some operations could make the socket to dispose itself
                _socket.Shutdown(SocketShutdown.Both);
                _socket.Close();
            }
            catch
            {
                //We should not mind if the socket shutdown or close methods throw an exception
            }
            if (_receiveSocketEvent != null)
            {
                //It is safe to call SocketAsyncEventArgs.Dispose() more than once
                //Also checked: .NET 4.0, .NET 4.5 and Mono 3.10 and 3.12 implementations
                _sendSocketEvent.Dispose();
                _receiveSocketEvent.Dispose();
            }
        }
    }
}
