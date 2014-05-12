using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Net;
using System.Net.Sockets;
using System.Text;
using System.Threading.Tasks;

namespace Cassandra
{
    /// <summary>
    /// Represents a tcp connection to a Cassandra Node
    /// </summary>
    public class Connection : IDisposable
    {
        protected Socket _socket;
        private BoolSwitch _isDisposed = new BoolSwitch();
        private BoolSwitch _isInitialized = new BoolSwitch();
        private ConcurrentStack<int> _freeOperations;
        private Dictionary<int, OperationState> _currentOperations;
        SocketAsyncEventArgs _receiveSocketEvent = new SocketAsyncEventArgs();
        SocketAsyncEventArgs _sendSocketEvent = new SocketAsyncEventArgs();
        public List<byte[]> _readBuffer { get; set; }

        protected virtual IPEndPoint IPEndPoint { get; set; }

        public virtual bool IsDisposed
        {
            get
            {
                return _isDisposed.IsTaken();
            }
        }

        protected virtual ProtocolOptions Options { get; set; }

        protected virtual SocketOptions SocketOptions { get; set; }

        public Connection(IPEndPoint endpoint, ProtocolOptions options, SocketOptions socketOptions)
        {
            this.IPEndPoint = endpoint;
            this.Options = options;
            this.SocketOptions = socketOptions;
        }

        public virtual void Dispose()
        {
            if (!_isDisposed.TryTake())
            {
                return;
            }
            _socket.Close();
        }

        /// <summary>
        /// Initializes the connection
        /// </summary>
        /// <exception cref="SocketException" />
        public virtual void Init()
        {
            if (!_isInitialized.TryTake())
            {
                return;
            }
            //Cassandra supports up to 128 concurrent requests
            _freeOperations = new ConcurrentStack<int>(Enumerable.Range(0, 128));
            _currentOperations = new Dictionary<int, OperationState>();
            _sendSocketEvent.Completed += OnSendCompleted;
            _receiveSocketEvent.SetBuffer(new byte[256], 0, 256);
            _receiveSocketEvent.Completed += OnReceiveCompleted;

            InitSocket();

            //Connect
            var connectResult = _socket.BeginConnect(IPEndPoint, null, null);
            connectResult.AsyncWaitHandle.WaitOne(SocketOptions.ConnectTimeoutMillis);

            if (!_socket.Connected)
            {
                //It timed out: Close the socket and throw the exception
                _socket.Close();
                throw new SocketException((int)SocketError.TimedOut);
            }
        }

        protected virtual void InitSocket()
        {
            _socket = new Socket(AddressFamily.InterNetwork, SocketType.Stream, ProtocolType.Tcp);
            if (SocketOptions.KeepAlive != null)
            {
                _socket.SetSocketOption(SocketOptionLevel.Socket, SocketOptionName.KeepAlive, SocketOptions.KeepAlive.Value);
            }

            _socket.SendTimeout = SocketOptions.ConnectTimeoutMillis;
            if (SocketOptions.SoLinger != null)
            {
                _socket.SetSocketOption(SocketOptionLevel.Socket, SocketOptionName.Linger, new LingerOption(true, SocketOptions.SoLinger.Value));
            }
            if (SocketOptions.ReceiveBufferSize != null)
            {
                _socket.ReceiveBufferSize = SocketOptions.ReceiveBufferSize.Value;
            }
            if (SocketOptions.SendBufferSize != null)
            {
                _socket.ReceiveBufferSize = SocketOptions.SendBufferSize.Value;
            }
            if (SocketOptions.TcpNoDelay != null)
            {
                _socket.NoDelay = SocketOptions.TcpNoDelay.Value;
            }
        }

        protected virtual void OnReceiveCompleted(object sender, SocketAsyncEventArgs e)
        {
            if(e.SocketError == SocketError.Success)
            {
                //TODO: Get the stream id and get it from the dictionary
                var streamId = _currentOperations.Keys.First();
                var state = _currentOperations[streamId];
                //TODO: Read frame header / frame length

                //TODO: if message finished
                var buffer = new byte[e.BytesTransferred];
                Buffer.BlockCopy(e.Buffer, 0, buffer, 0, e.BytesTransferred);
                state.TaskCompletionSource.TrySetResult(buffer);
                //Remove from the internal state
                _currentOperations.Remove(streamId);
            }
        }

        protected virtual void OnSendCompleted(object sender, SocketAsyncEventArgs e)
        {
             if (e.SocketError == SocketError.Success)
             {
                 //Begin read
                _receiveSocketEvent.SetBuffer(new byte[1024], 0, 1024);
                var willRaiseEvent = _socket.ReceiveAsync(_receiveSocketEvent);
                if (!willRaiseEvent)
                {
                    OnReceiveCompleted(this, _receiveSocketEvent);
                }
             }
             else
             {
                 //TODO: Get the streamId and Raise exception
             }
        }

        protected internal virtual Task<object> Startup()
        {
            int streamId = -1;
            if (!_freeOperations.TryPop(out streamId))
            {
                //TODO: Throw a BusyException
            }
            var request = new StartupRequest(streamId, new Dictionary<string, string>() { { "CQL_VERSION", "3.0.0" } });
            var buffer = request.GetFrame(1).Buffer.ToArray();
            var state = new OperationState()
            {
                TaskCompletionSource = new TaskCompletionSource<object>()
            };
            _currentOperations.Add(streamId, state);

            Write(_sendSocketEvent, request.GetFrame(1).Buffer.ToArray());
            return state.TaskCompletionSource.Task;
        }

        protected internal virtual void Write(SocketAsyncEventArgs sendEvent, byte[] buffer)
        {
            sendEvent.SetBuffer(buffer, 0, buffer.Length);
            var willRaiseEvent  = _socket.SendAsync(sendEvent);
            if (!willRaiseEvent)
            {
                OnSendCompleted(this, sendEvent);
            }
        }
    }

    internal class OperationState 
    {
        public TaskCompletionSource<object> TaskCompletionSource { get; set;}
        
        public int FrameLength { get; set; }

        public int BytesRead { get; set; }
    }
}
