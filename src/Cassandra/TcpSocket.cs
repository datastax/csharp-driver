using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Net;
using System.Net.Sockets;
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
        private Socket _socket;
        private SocketAsyncEventArgs _receiveSocketEvent;
        private SocketAsyncEventArgs _sendSocketEvent;
        
        public IPEndPoint IPEndPoint { get; protected set; }

        public SocketOptions Options { get; protected set; }

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
        public TcpSocket(IPEndPoint ipEndPoint, SocketOptions options)
        {
            this.IPEndPoint = ipEndPoint;
            this.Options = options;
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
            _receiveSocketEvent = new SocketAsyncEventArgs();
            _receiveSocketEvent.SetBuffer(new byte[_socket.ReceiveBufferSize], 0, _socket.ReceiveBufferSize);
            _receiveSocketEvent.Completed += OnReceiveCompleted;
            _sendSocketEvent = new SocketAsyncEventArgs();
            _sendSocketEvent.Completed += OnSendCompleted;
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

            ReceiveAsync();
        }

        /// <summary>
        /// Begins an asynchronous request to receive data from a connected Socket object.
        /// It handles the exceptions in case there is one.
        /// </summary>
        protected virtual void ReceiveAsync()
        {
            //Receive the next bytes
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

        protected virtual void OnError(Exception ex, SocketError? socketError = null)
        {
            if (Error != null)
            {
                Error(ex, socketError);
            }
        }

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

        protected virtual void OnClosing()
        {
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
            //This can result in OOM
            //A neat improvement would be to write this sync in small buffers when buffer.length > X
            var buffer = new byte[stream.Length];
            stream.Position = 0;
            stream.Read(buffer, 0, buffer.Length);

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
