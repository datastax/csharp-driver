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

using System;
using System.IO;
using System.Net;
using System.Net.Security;
using System.Net.Sockets;
using System.Threading;
using System.Threading.Tasks;
using Cassandra.Tasks;
using Microsoft.IO;

namespace Cassandra
{
    /// <summary>
    /// Represents a Tcp connection to a host.
    /// It emits Read and WriteCompleted events when data is received.
    /// Similar to Netty's Channel or Node.js's net.Socket
    /// It handles TLS validation and encryption when required.
    /// </summary>
    internal class TcpSocket : IDisposable
    {
        private static Logger _logger = new Logger(typeof(TcpSocket));
        private Socket _socket;
        private SocketAsyncEventArgs _receiveSocketEvent;
        private SocketAsyncEventArgs _sendSocketEvent;
        private Stream _socketStream;
        private byte[] _receiveBuffer;
        private volatile bool _isClosing;
        private Action _writeFlushCallback;

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
        /// Connects asynchronously to the host and starts reading
        /// </summary>
        /// <exception cref="SocketException">Throws a SocketException when the connection could not be established with the host</exception>
        public async Task<bool> Connect()
        {
            var tcs = TaskHelper.TaskCompletionSourceWithTimeout<bool>(
                Options.ConnectTimeoutMillis, 
                () => new SocketException((int) SocketError.TimedOut));
            var socketConnectTask = tcs.Task;
            var eventArgs = new SocketAsyncEventArgs
            {
                RemoteEndPoint = IPEndPoint
            };

            eventArgs.Completed += (sender, e) =>
            {
                if (e.SocketError != SocketError.Success)
                {
                    tcs.TrySetException(new SocketException((int)e.SocketError));
                    return;
                }
                tcs.TrySetResult(true);
                e.Dispose();
            };
            try
            {
                _socket.ConnectAsync(eventArgs);
                await socketConnectTask.ConfigureAwait(false);
            }
            finally
            {
                eventArgs.Dispose();
            }
            if (SSLOptions != null)
            {
                return await ConnectSsl().ConfigureAwait(false);
            }
            // Prepare read and write
            // There are 2 modes: using SocketAsyncEventArgs (most performant) and Stream mode
            if (Options.UseStreamMode)
            {
                _logger.Verbose("Socket connected, start reading using Stream interface");
                //Stream mode: not the most performant but it is a choice
                _socketStream = new NetworkStream(_socket);
                ReceiveAsync();
                return true;
            }
            _logger.Verbose("Socket connected, start reading using SocketEventArgs interface");
            //using SocketAsyncEventArgs
            _receiveSocketEvent = new SocketAsyncEventArgs();
            _receiveSocketEvent.SetBuffer(_receiveBuffer, 0, _receiveBuffer.Length);
            _receiveSocketEvent.Completed += OnReceiveCompleted;
            _sendSocketEvent = new SocketAsyncEventArgs();
            _sendSocketEvent.Completed += OnSendCompleted;
            ReceiveAsync();
            return true;
        }

        private async Task<bool> ConnectSsl()
        {
            _logger.Verbose("Socket connected, starting SSL client authentication");
            //Stream mode: not the most performant but it has ssl support
            var targetHost = IPEndPoint.Address.ToString();
            //HostNameResolver is a sync operation but it can block
            //Use another thread
            Action resolveAction = () =>
            {
                try
                {
                    targetHost = SSLOptions.HostNameResolver(IPEndPoint.Address);
                }
                catch (Exception ex)
                {
                    _logger.Error(
                        string.Format(
                            "SSL connection: Can not resolve host name for address {0}. Using the IP address instead of the host name. This may cause RemoteCertificateNameMismatch error during Cassandra host authentication. Note that the Cassandra node SSL certificate's CN(Common Name) must match the Cassandra node hostname.",
                            targetHost), ex);
                }
            };
            await Task.Factory.StartNew(resolveAction).ConfigureAwait(false);

            _logger.Verbose("Starting SSL authentication");
            var sslStream = new SslStream(new NetworkStream(_socket), false, SSLOptions.RemoteCertValidationCallback, null);
            _socketStream = sslStream;
            // Use a timer to ensure that it does callback
            var tcs = TaskHelper.TaskCompletionSourceWithTimeout<bool>(
                Options.ConnectTimeoutMillis,
                () => new TimeoutException("The timeout period elapsed prior to completion of SSL authentication operation."));

            sslStream.AuthenticateAsClientAsync(targetHost,
                                                SSLOptions.CertificateCollection,
                                                SSLOptions.SslProtocol,
                                                SSLOptions.CheckCertificateRevocation)
                     .ContinueWith(t =>
                     {
                         if (t.Exception != null)
                         {
                             t.Exception.Handle(_ => true);
                             // ReSharper disable once AssignNullToNotNullAttribute
                             tcs.TrySetException(t.Exception.InnerException);
                             return;
                         }
                         tcs.TrySetResult(true);
                     }, TaskContinuationOptions.ExecuteSynchronously)
                     // Avoid awaiting as it may never yield
                     .Forget();

            await tcs.Task.ConfigureAwait(false);
            _logger.Verbose("SSL authentication successful");
            ReceiveAsync();
            return true;
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
                catch (ObjectDisposedException)
                {
                    OnError(null, SocketError.NotConnected);
                }
                catch (NullReferenceException)
                {
                    // Mono can throw a NRE when being disposed concurrently
                    // https://github.com/mono/mono/blob/b190f213a364a2793cc573e1bd9fae8be72296e4/mcs/class/System/System.Net.Sockets/SocketAsyncEventArgs.cs#L184-L185
                    // https://github.com/mono/mono/blob/b190f213a364a2793cc573e1bd9fae8be72296e4/mcs/class/System/System.Net.Sockets/Socket.cs#L1873-L1874
                    OnError(null, SocketError.NotConnected);
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
                // Stream mode
                try
                {
                    _socketStream
                        .ReadAsync(_receiveBuffer, 0, _receiveBuffer.Length)
                        .ContinueWith(OnReceiveStreamCallback, TaskContinuationOptions.ExecuteSynchronously);
                }
                catch (Exception ex)
                {
                    HandleStreamException(ex);
                }
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
        /// Handles the callback for Completed or Cancelled Task on Stream mode
        /// </summary>
        protected void OnReceiveStreamCallback(Task<int> readTask)
        {
            if (readTask.Exception != null)
            {
                readTask.Exception.Handle(_ => true);
                HandleStreamException(readTask.Exception.InnerException);
                return;
            }
            var bytesRead = readTask.Result;
            if (bytesRead == 0)
            {
                OnClosing();
                return;
            }
            //Emit event
            try
            {
                if (Read != null)
                {
                    Read(_receiveBuffer, bytesRead);
                }
            }
            catch (Exception ex)
            {
                OnError(ex);
            }
            ReceiveAsync();
        }

        /// <summary>
        /// Handles exceptions that the methods <c>NetworkStream.ReadAsync()</c> and <c>NetworkStream.WriteAsync()</c> can throw.
        /// </summary>
        private void HandleStreamException(Exception ex)
        {
            if (ex is IOException)
            {
                if (ex.InnerException is SocketException)
                {
                    OnError((SocketException)ex.InnerException);
                    return;
                }
                // Wrapped ObjectDisposedException and others: we can consider it as not connected
                OnError(null, SocketError.NotConnected);
                return;
            }
            if (ex is ObjectDisposedException)
            {
                // Wrapped ObjectDisposedException and others: we can consider it as not connected
                OnError(null, SocketError.NotConnected);
                return;
            }
            OnError(ex);
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
            OnWriteFlushed();
            if (WriteCompleted != null)
            {
                WriteCompleted();
            }
        }

        /// <summary>
        /// Handles the continuation for WriteAsync faulted or Task on Stream mode
        /// </summary>
        protected void OnSendStreamCallback(Task writeTask)
        {
            if (writeTask.Exception != null)
            {
                writeTask.Exception.Handle(_ => true);
                HandleStreamException(writeTask.Exception.InnerException);
                return;
            }
            OnWriteFlushed();
            if (WriteCompleted != null)
            {
                WriteCompleted();
            }
        }

        protected void OnClosing()
        {
            _isClosing = true;
            if (Closing != null)
            {
                Closing.Invoke();
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

        private void OnWriteFlushed()
        {
            var callback = Interlocked.Exchange(ref _writeFlushCallback, null);
            if (callback != null)
            {
                callback();
            }
        }

        /// <summary>
        /// Sends data asynchronously
        /// </summary>
        public void Write(RecyclableMemoryStream stream, Action onBufferFlush)
        {
            Interlocked.Exchange(ref _writeFlushCallback, onBufferFlush);
            if (_isClosing)
            {
                OnError(new SocketException((int)SocketError.Shutdown));
                OnWriteFlushed();
                return;
            }
            if (_sendSocketEvent != null)
            {
                _sendSocketEvent.BufferList = stream.GetBufferList();
                var isWritePending = false;
                try
                {
                    isWritePending = _socket.SendAsync(_sendSocketEvent);
                }
                catch (ObjectDisposedException)
                {
                    OnError(null, SocketError.NotConnected);
                }
                catch (NullReferenceException)
                {
                    // Mono can throw a NRE when being disposed concurrently
                    // https://github.com/mono/mono/blob/b190f213a364a2793cc573e1bd9fae8be72296e4/mcs/class/System/System.Net.Sockets/SocketAsyncEventArgs.cs#L184-L185
                    // https://github.com/mono/mono/blob/b190f213a364a2793cc573e1bd9fae8be72296e4/mcs/class/System/System.Net.Sockets/Socket.cs#L2477-L2478
                    OnError(null, SocketError.NotConnected);
                }
                catch (Exception ex)
                {
                    OnError(ex);
                }
                if (!isWritePending)
                {
                    OnSendCompleted(this, _sendSocketEvent);
                }
            }
            else
            {
                var length = (int)stream.Length;
                try
                {
                    _socketStream
                        .WriteAsync(stream.GetBuffer(), 0, length)
                        .ContinueWith(OnSendStreamCallback, TaskContinuationOptions.ExecuteSynchronously);
                }
                catch (Exception ex)
                {
                    HandleStreamException(ex);
                }
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
            }
            catch
            {
                // Shutdown might throw an exception if the socket was not open-open
            }
            try
            {
                _socket.Dispose();
            }
            catch
            {
                //We should not mind if socket's Close method throws an exception
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