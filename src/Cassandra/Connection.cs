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
    internal class Connection : IDisposable
    {
        private Logger _logger = new Logger(typeof(Connection));
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
        /// <summary>
        /// Small buffer (less than 8 bytes) that is used when the next received message is smaller than 8 bytes, 
        /// and it is not possible to read the header.
        /// </summary>
        private byte[] _minimalBuffer;

        public IFrameCompressor Compressor { get; set; }

        protected virtual ProtocolOptions Options { get; set; }

        public byte ProtocolVersion { get; set; }

        public Connection(byte protocolVersion, IPEndPoint endpoint, ProtocolOptions options, SocketOptions socketOptions)
        {
            this.ProtocolVersion = protocolVersion;
            this.Options = options;
            _tcpSocket = new TcpSocket(endpoint, socketOptions);
        }

        /// <summary>
        /// It callbacks all operations already sent / or to be written, that do not have a response.
        /// </summary>
        protected virtual void CancelPending(Exception ex, SocketError? socketError = null)
        {
            //TODO: Make it thread safe.
            _logger.Info("Canceling pending operations " + _pendingOperations.Count + " and write queue " + _writeQueue.Count);
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
        /// <exception cref="SocketException">Throws a SocketException when the connection could not be established with the host</exception>
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

            //MAYBE: Allow the possibility to provide a custom provider
            if (Options.Compression == CompressionType.LZ4)
            {
                Compressor = new LZ4Compressor();
            }
            else if (Options.Compression == CompressionType.Snappy)
            {
                Compressor = new SnappyCompressor();
            }

            //Init TcpSocket
            _tcpSocket.Init();
            _tcpSocket.Error += CancelPending;
            _tcpSocket.Closing += () => CancelPending(null, null);
            _tcpSocket.Read += ReadHandler;
            _tcpSocket.WriteCompleted += WriteCompletedHandler;
            _tcpSocket.Connect();
        }

        /// <summary>
        /// Sends a protocol Prepare message
        /// </summary>
        public virtual Task<ResultResponse> Prepare(string query)
        {
            var request = new PrepareRequest(query);
            var responseSource = new ResponseSource<ResultResponse>();
            Send(request, responseSource);
            return responseSource.Task;
        }

        /// <summary>
        /// Sends a protocol Query message
        /// </summary>
        public virtual Task<ResultResponse> Query(string query, QueryProtocolOptions queryOptions, bool tracing = false, ConsistencyLevel? consistency = null)
        {
            var request = new QueryRequest(query, tracing, queryOptions, consistency);
            var responseSource = new ResponseSource<ResultResponse>();
            Send(request, responseSource);
            return responseSource.Task;
        }

        protected internal virtual void ReadHandler(byte[] buffer, int bytesReceived)
        {
            //Parse the data received
            var streamIdAvailable = ReadParse(buffer, 0, bytesReceived);
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
        /// <param name="buffer">Byte buffer to read</param>
        /// <param name="offset">Offset within the buffer</param>
        /// <param name="count">Length of bytes to be read from the buffer</param>
        /// <returns>True if a full operation (streamId) has been processed.</returns>
        protected virtual bool ReadParse(byte[] buffer, int offset, int count)
        {
            OperationState state = _receivingOperation;
            if (state == null)
            {
                if (_minimalBuffer != null)
                {
                    buffer = Utils.JoinBuffers(_minimalBuffer, 0, _minimalBuffer.Length, buffer, offset, count);
                    offset = 0;
                    count = buffer.Length;
                }
                if (count < 8)
                {
                    //There is not enough data to read the header
                    _minimalBuffer = Utils.SliceBuffer(buffer, offset, count);
                    return false;
                }
                _minimalBuffer = null;
                var header = FrameHeader.ParseResponseHeader(buffer, offset);
                //Check if its a response
                if (header.Version >> 9 != 1 && (header.Version & 0x07) == 0)
                {
                    _logger.Error("Not a response header");
                }
                offset += FrameHeader.Size;
                count -= FrameHeader.Size;
                state = _pendingOperations[header.StreamId];
                state.Header = header;
                _receivingOperation = state;
            }
            var countAdded = state.AppendBody(buffer, offset, count);

            if (state.IsBodyComplete)
            {
                _logger.Verbose("Read #" + state.Header.StreamId);
                //Stop reference it as the current receiving operation
                _receivingOperation = null;
                //Remove from pending
                _pendingOperations.TryRemove(state.Header.StreamId, out state);
                //Release the streamId
                _freeOperations.Push(state.Header.StreamId);
                try
                {
                    var response = ReadParseResponse(state.Header, state.BodyStream);
                    state.ResponseSource.SetResponse(response);
                }
                catch (Exception ex)
                {
                    state.ResponseSource.SetException(ex);
                }

                if (countAdded < count)
                {
                    //There is more data, from the next frame
                    ReadParse(buffer, offset + countAdded, count - countAdded);
                }
                return true;
            }
            //There isn't enough data to read the whole frame.
            //It is already buffered, carry on.
            return false;
        }

        private AbstractResponse ReadParseResponse(FrameHeader header, Stream body)
        {
            //Start at the first byte
            body.Position = 0;
            if ((header.Flags & 0x01) > 0)
            {
                body = Compressor.Decompress(body);
            }
            var frame = new ResponseFrame(header, body);
            var response = FrameParser.Parse(frame);
            return response;
        }

        /// <summary>
        /// Sends a protocol startup message
        /// </summary>
        internal virtual Task Startup()
        {
            var startupOptions = new Dictionary<string, string>();
            startupOptions.Add("CQL_VERSION", "3.0.0");
            if (Options.Compression == CompressionType.LZ4)
            {
                startupOptions.Add("COMPRESSION", "lz4");
            }
            else if (Options.Compression == CompressionType.Snappy)
            {
                startupOptions.Add("COMPRESSION", "snappy");
            }
            var request = new StartupRequest(startupOptions);
            var responseSource = new ResponseSource();
            Send(request, responseSource);
            return responseSource.Task;
        }

        /// <summary>
        /// Sends a new request if possible. If it is not possible it queues it up.
        /// </summary>
        internal virtual void Send(IRequest request, IResponseSource responseSource)
        {
            //thread safe write queue
            var state = new OperationState
            {
                Request = request,
                ResponseSource = responseSource
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
                    _logger.Verbose("Enqueued: " + _writeQueue.Count);
                    return;
                }
                //Prevent the next to process
                _canWriteNext = false;
            }
            
            //At this point:
            //We have a valid stream id
            //Only 1 thread at a time can be here.
            _pendingOperations.AddOrUpdate(streamId, state, (k, oldValue) => state);

            var frameStream = state.Request.GetFrame((byte)streamId, ProtocolVersion).Stream;
            //We will not use the request, stop reference it.
            state.Request = null;
            //Start sending it
            _tcpSocket.Write(frameStream);
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
