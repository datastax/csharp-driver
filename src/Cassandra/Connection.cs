using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Net;
using System.Net.Sockets;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace Cassandra
{
    /// <summary>
    /// Represents a TCP connection to a Cassandra Node
    /// </summary>
    public class Connection : IDisposable
    {
        private TcpSocket _tcpSocket;
        private BoolSwitch _isDisposed = new BoolSwitch();
        private BoolSwitch _isInitialized = new BoolSwitch();
        /// <summary>
        /// Stores the available stream ids.
        /// </summary>
        private ConcurrentStack<int> _freeOperations;
        /// <summary>
        /// Contains the requests that were sent through the wire and that hasn't been received yet.
        /// </summary>
        private ConcurrentDictionary<int, OperationState> _pendingOperations;
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

        protected virtual ProtocolOptions Options { get; set; }

        public Connection(IPEndPoint endpoint, ProtocolOptions options, SocketOptions socketOptions)
        {
            this.Options = options;
            _tcpSocket = new TcpSocket(endpoint, socketOptions);
        }

        /// <summary>
        /// It callbacks all operations already sent / or to be written, that do not have a response.
        /// </summary>
        protected virtual void CancelPending(Exception ex, SocketError? socketError = null)
        {
            //TODO: Make it thread safe.
            if (_pendingOperations.Count > 0)
            {
                //TODO: Callback every pending operation
                throw new NotImplementedException();
            }
            if (_writeQueue.Count > 0)
            {
                //TODO: Callback all the items in the write queue
                throw new NotImplementedException();
            }
        }

        public virtual void Dispose()
        {
            if (!_isDisposed.TryTake())
            {
                return;
            }
            _tcpSocket.Dispose();
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

            //Init TcpSocket
            _tcpSocket.Init();
            _tcpSocket.Error += CancelPending;
            _tcpSocket.Closing += () => CancelPending(null, null);
            _tcpSocket.Read += ReadHandler;
            _tcpSocket.WriteCompleted += WriteCompletedHandler;
            _tcpSocket.Connect();
        }

        internal virtual Task<AbstractResponse> Query()
        {
            var request = new QueryRequest("SELECT * FROM system.schema_keyspaces", false, QueryProtocolOptions.Default, null);
            var tcs = new TaskCompletionSource<AbstractResponse>();
            Send(request, tcs);
            return tcs.Task;
        }

        protected internal virtual void ReadHandler(byte[] buffer, int bytesReceived)
        {
            //TODO: Defer copy
            var newBuffer = new byte[bytesReceived];
            Buffer.BlockCopy(buffer, 0, newBuffer, 0, bytesReceived);

            //Parse the data received
            var streamIdAvailable = ReadParse(newBuffer);
            if (streamIdAvailable)
            {
                //Process a next item in the queue if possible.
                //Maybe there are there items in the write queue that were waiting on a fresh streamId
                SendQueueNext();
            }
        }

        /// <summary>
        /// Parses the bytes received into a frame. Uses the internal operation state to do the callbacks.
        /// Returns true if a full operation (streamId) has been processed and there is one available.
        /// </summary>
        /// <returns>True if a full operation (streamId) has been processed.</returns>
        protected virtual bool ReadParse(byte[] buffer)
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
                //Stop reference it as the current receiving operation
                _receivingOperation = null;
                //Remove from pending
                _pendingOperations.TryRemove(state.Header.StreamId, out state);
                //Release the streamId
                _freeOperations.Push(state.Header.StreamId);
                try
                {
                    var response = ReadParseResponse(state.Header, state.ReadBuffer);
                    state.TaskCompletionSource.TrySetResult(response);
                }
                catch (Exception ex)
                {
                    state.TaskCompletionSource.TrySetException(ex);
                }

                if (state.ReadBuffer.Length > state.Header.TotalFrameLength)
                {
                    //There is more data, from the next frame
                    int nextBufferSize = state.ReadBuffer.Length - state.Header.TotalFrameLength;
                    var nextFrameBuffer = Utils.SliceBuffer(state.ReadBuffer, state.Header.TotalFrameLength, nextBufferSize);
                    ReadParse(nextFrameBuffer);
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

        private AbstractResponse ReadParseResponse(FrameHeader header, byte[] buffer)
        {
            var stream = new MemoryStream(buffer, 8, header.BodyLength, false);
            var frame = new ResponseFrame(header, stream);
            var response = FrameParser.Parse(frame);
            return response;
        }

        /// <summary>
        /// Sends a protocol startup message
        /// </summary>
        internal virtual Task<AbstractResponse> Startup()
        {
            var request = new StartupRequest(new Dictionary<string, string>() { { "CQL_VERSION", "3.0.0" } });
            var tcs = new TaskCompletionSource<AbstractResponse>();
            Send(request, tcs);
            return tcs.Task;
        }

        /// <summary>
        /// Sends a new request if possible. If it is not possible it queues it up.
        /// </summary>
        internal virtual void Send(IRequest request, TaskCompletionSource<AbstractResponse> tcs)
        {
            //thread safe write queue
            var state = new OperationState
            {
                Request = request,
                TaskCompletionSource = tcs
            };
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

            //TODO: Remove memory tributary
            var buffer = state.Request.GetFrame((byte)streamId, 1).Buffer.ToArray();
            //We will not use the request, stop reference it.
            state.Request = null;
            //Start sending it
            _tcpSocket.Write(buffer);
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

        /// <summary>
        /// Method that gets executed when a write request has been completed.
        /// </summary>
        protected internal virtual void WriteCompletedHandler()
        {
            //There is no need to lock
            //Only 1 thread can be here at the same time.
            _canWriteNext = true;
            SendQueueNext();
        }
    }
}
