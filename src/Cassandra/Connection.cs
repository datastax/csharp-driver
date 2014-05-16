using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Net;
using System.Net.Sockets;
using System.Text;
using System.Threading;
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
        /// <summary>
        /// Stores the available stream ids.
        /// </summary>
        private ConcurrentStack<int> _freeOperations;
        /// <summary>
        /// Contains the requests that were sent through the wire and that hasnt been received yet.
        /// </summary>
        private ConcurrentDictionary<int, OperationState> _pendingOperations;
        private SocketAsyncEventArgs _receiveSocketEvent = new SocketAsyncEventArgs();
        private SocketAsyncEventArgs _sendSocketEvent = new SocketAsyncEventArgs();
        /// <summary>
        /// It determines if the write queue can process the next (if it is not in-flight).
        /// It has to be volatile as it can not be cached by the thread.
        /// </summary>
        private volatile bool _canWriteNext = true;
        /// <summary>
        /// Its for processing the next item in the write queue.
        /// It can not be replaced by a Interlocked Increment as it must allow rollbacks (when there are no stream ids left).
        /// </summary>
        private object _writeQueueLock = new object();
        private ConcurrentQueue<OperationState> _writeQueue;
        private OperationState _receivingOperation;
        private byte[] _minimalBuffer;

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

        protected virtual void CancelPending(SocketError error)
        {
            if (_pendingOperations.Count > 0)
            {
                //TODO: Callback every pending operation
                throw new NotImplementedException();
            }
        }

        public virtual void Dispose()
        {
            if (!_isDisposed.TryTake())
            {
                return;
            }
            try
            {
                //Try to close it.
                //Some operations could make the socket to dispose itself
                //We should not mind if the socket shutdown or close throws an exception
                _socket.Shutdown(SocketShutdown.Both);
                _socket.Close(); 
            }
            catch
            {

            }
        }

        /// <summary>
        /// Initializes the connection. Thread safe.
        /// </summary>
        /// <exception cref="SocketException">Throws a SocketException when the connection could not be stablished with the host</exception>
        public virtual void Init()
        {
            if (!_isInitialized.TryTake())
            {
                //MAYBE: If really necessary, we can Wait on the BeginConnect result.
                return;
            }
            //Cassandra supports up to 128 concurrent requests
            _freeOperations = new ConcurrentStack<int>(Enumerable.Range(0, 128));
            _pendingOperations = new ConcurrentDictionary<int, OperationState>();
            _writeQueue = new ConcurrentQueue<OperationState>();
            _sendSocketEvent.Completed += OnSendCompleted;
            _receiveSocketEvent.SetBuffer(new byte[10240], 0, 10240);
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

            //Start receiving
            //TODO: Catch possible exceptions
            var willRaiseEvent = _socket.ReceiveAsync(_receiveSocketEvent);
            if (!willRaiseEvent)
            {
                OnReceiveCompleted(this, _receiveSocketEvent);
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
            if(e.SocketError != SocketError.Success || e.BytesTransferred == 0)
            {
                //There was a socket error or the connection is being closed.
                CancelPending(e.SocketError);
            }
            //MAYBE: Possible improve by deferring copy
            var buffer = new byte[e.BytesTransferred];
            Buffer.BlockCopy(e.Buffer, 0, buffer, 0, e.BytesTransferred);
            //Parse the data received
            ReceiveParse(buffer);
            //TODO: Determine if there are new streams ids available
            //If so, call SendQueueNext();
                
            //Receive the next bytes
            //TODO: Catch possible exceptions
            var willRaiseEvent = _socket.ReceiveAsync(_receiveSocketEvent);
            if (!willRaiseEvent)
            {
                OnReceiveCompleted(this, _receiveSocketEvent);
            }
        }

        protected internal virtual void OnSendCompleted(object sender, SocketAsyncEventArgs e)
        {
             if (e.SocketError == SocketError.Success)
             {
                 //There is no need to lock
                 //Only 1 thread can be here at the same time.
                 _canWriteNext = true;
                 SendQueueNext();
             }
             else
             {
                 CancelPending(e.SocketError);
             }
        }

        protected internal virtual Task<object> Query()
        {
            var request = new QueryRequest("SELECT * FROM system.schema_keyspaces", false, QueryProtocolOptions.Default, null);
            var tcs = new TaskCompletionSource<object>();
            Send(request, tcs);
            return tcs.Task;
        }

        /// <summary>
        /// Parses the bytes received into a frame. Uses the internal operation state to do the callbacks.
        /// </summary>
        /// <returns>True if a full operation (streamId) has been processed.</returns>
        private bool ReceiveParse(byte[] buffer)
        {
            OperationState state = _receivingOperation;
            if (state == null)
            {
                buffer = Utils.JoinBuffers(_minimalBuffer, buffer);
                if (buffer.Length < 8)
                {
                    //There is not enough data to read the header
                    _minimalBuffer = buffer;
                    return false;
                }
                _minimalBuffer = null;
                var header = FrameHeader.Parse(buffer);
                state = _pendingOperations[header.StreamId];
                state.Header = header;
                _receivingOperation = state;
            }
            state.AddBuffer(buffer);

            if (state.IsBodyComplete)
            {
                _receivingOperation = null;
                //Remove from pending
                _pendingOperations.TryRemove(state.Header.StreamId, out state);

                //Release the streamId
                _freeOperations.Push(state.Header.StreamId);
                //TODO: Parse body
                state.TaskCompletionSource.TrySetResult(state.ReadBuffer);

                if (state.ReadBuffer.Length > state.Header.TotalFrameLength)
                {
                    //There is more data, from the next frame
                    int nextBufferSize = state.ReadBuffer.Length - state.Header.TotalFrameLength;
                    var nextFrameBuffer = Utils.SliceBuffer(state.ReadBuffer, state.Header.TotalFrameLength, nextBufferSize);
                    ReceiveParse(nextFrameBuffer);
                }
                return true;
            }
            else
            {
                //There isn't enough data to read the whole frame.
                //It is already buffered, carry on.
            }
            return false;
        }

        /// <summary>
        /// Sends a protocol startup message
        /// </summary>
        protected internal virtual Task<object> Startup()
        {
            var request = new StartupRequest(new Dictionary<string, string>() { { "CQL_VERSION", "3.0.0" } });
            var tcs = new TaskCompletionSource<object>();
            Send(request, tcs);
            return tcs.Task;
        }

        /// <summary>
        /// Sends a new request if possible. If it is not possible it queues it up.
        /// </summary>
        protected internal virtual void Send(IRequest request, TaskCompletionSource<object> tcs)
        {
            //thread safe write queue
            var state = new OperationState
            {
                Request = request,
                TaskCompletionSource = tcs
            };
            //Queue it
            SendQueueProcess(state);
        }

        /// <summary>
        /// Try to write the item provided. Thread safe.
        /// </summary>
        private void SendQueueProcess(OperationState state)
        {
            if (!_canWriteNext)
            {
                //Double-checked locking for best performance
                _writeQueue.Enqueue(state);
                return;
            }
            int streamId = -1;
            lock (_writeQueueLock)
            {
                if (!_canWriteNext)
                {
                    //We have to recheck as the world can change since the last instruction
                    _writeQueue.Enqueue(state);
                    return;
                }
                //Check if Cassandra can process a new operation
                if (!_freeOperations.TryPop(out streamId))
                {
                    //Queue it up for later.
                    //When receiving the next complete message, we can process it.
                    _writeQueue.Enqueue(state);
                    //MAYBE: We could BusyException
                    return;
                }
                //Prevent the next to process
                _canWriteNext = false;
            }
            
            //At this point:
            //We have a valid stream id
            //Only 1 thread at a time can be here.
            _pendingOperations.AddOrUpdate(streamId, state, (k, oldValue) => state);

            //TODO: Catch exceptions of GetFrame and rollback it.
            //TODO: Remove memory tributary
            var buffer = state.Request.GetFrame((byte)streamId, 1).Buffer.ToArray();
            //We will not use the request, stop reference it.
            state.Request = null;
            //In case there is a send problem, we could recover the streamId from the UserToken
            _sendSocketEvent.UserToken = streamId;
            //Start sending it
            Write(_sendSocketEvent, buffer);
        }

        /// <summary>
        /// Try to write the next item in the write queue. Thread safe.
        /// </summary>
        protected virtual void SendQueueNext()
        {
            if (_canWriteNext)
            {
                OperationState state;
                if (_writeQueue.TryDequeue(out state))
                {
                    SendQueueProcess(state);
                }
            }
        }

        protected internal virtual void Write(SocketAsyncEventArgs sendEvent, byte[] buffer)
        {
            sendEvent.SetBuffer(buffer, 0, buffer.Length);
            //TODO: Catch possible exceptions
            var willRaiseEvent  = _socket.SendAsync(sendEvent);
            if (!willRaiseEvent)
            {
                OnSendCompleted(this, sendEvent);
            }
        }
    }
}
