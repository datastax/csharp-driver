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
        private const byte _maxConcurrentRequests = 128;
        private TcpSocket _tcpSocket;
        private BoolSwitch _isDisposed = new BoolSwitch();
        /// <summary>
        /// Determines that the connection canceled pending operations.
        /// It could be because its being closed or there was a socket error.
        /// </summary>
        private volatile bool _isCanceled;
        private object _cancelLock = new object();
        private AutoResetEvent _pendingWaitHandle;
        /// <summary>
        /// Stores the available stream ids.
        /// </summary>
        private ConcurrentStack<byte> _freeOperations;
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
        private volatile string _keyspace;
        private object _keyspaceLock = new object();
        /// <summary>
        /// The event that represents a event RESPONSE from a Cassandra node
        /// </summary>
        public event CassandraEventHandler CassandraEventResponse;
        public IFrameCompressor Compressor { get; set; }

        public IPAddress HostAddress
        {
            get
            {
                return _tcpSocket.IPEndPoint.Address;
            }
        }

        /// <summary>
        /// Determines the amount of operations that are not finished.
        /// </summary>
        public int InFlight
        { 
            get
            {
                return _pendingOperations.Count;
            }
        }

        /// <summary>
        /// Gets or sets the keyspace.
        /// When setting the keyspace, it will issue a Query Request and wait to complete.
        /// </summary>
        public string Keyspace
        {
            get
            {
                return this._keyspace;
            }
            set
            {
                if (String.IsNullOrEmpty(value))
                {
                    return;
                }
                if (this._keyspace != null && value == this._keyspace)
                {
                    return;
                }
                lock (this._keyspaceLock)
                {
                    if (value == this._keyspace)
                    {
                        return;
                    }
                    _logger.Info("Connection to host " + HostAddress + " switching to keyspace " + value);
                    this._keyspace = value;
                    var timeout = Configuration.SocketOptions.ConnectTimeoutMillis;
                    var request = new QueryRequest("USE " + value, false, QueryProtocolOptions.Default);
                    TaskHelper.WaitToComplete(this.Send(request), timeout);
                }
            }
        }

        public ProtocolOptions Options { get { return Configuration.ProtocolOptions; } }

        public byte ProtocolVersion { get; set; }

        public Configuration Configuration { get; set; }

        public Connection(byte protocolVersion, IPEndPoint endpoint, Configuration configuration)
        {
            this.ProtocolVersion = protocolVersion;
            this.Configuration = configuration;
            _tcpSocket = new TcpSocket(endpoint, configuration.SocketOptions, configuration.ProtocolOptions.SslOptions);
        }

        /// <summary>
        /// Starts the authentication flow
        /// </summary>
        /// <exception cref="AuthenticationException" />
        private void Authenticate()
        {
            var provider = Configuration.AuthProvider;
            if (ProtocolVersion == 1 && provider == NoneAuthProvider.Instance)
            {
                //Use protocol v1 authentication flow
                var credentialsProvider = Configuration.AuthInfoProvider;
                var credentials = credentialsProvider.GetAuthInfos(HostAddress);
                var request = new CredentialsRequest(credentials);
                var response = TaskHelper.WaitToComplete(this.Send(request), Configuration.SocketOptions.ConnectTimeoutMillis);
                //If Cassandra replied with a auth response error
                //The task already is faulted and the exception was already thrown.
                if (response is ReadyResponse)
                {
                    return;
                }
                throw new ProtocolErrorException("Expected SASL response, obtained " + response.GetType().Name);
            }
            else
            {
                //Use protocol v2+ authentication flow
                var authenticator = provider.NewAuthenticator(HostAddress);

                byte[] initialResponse = authenticator.InitialResponse();
                if (null == initialResponse)
                {
                    initialResponse = new byte[0];
                }
                Authenticate(initialResponse, authenticator);
            }
        }

        /// <exception cref="AuthenticationException" />
        private void Authenticate(byte[] token, IAuthenticator authenticator)
        {
            var request = new AuthResponseRequest(token);
            var response = TaskHelper.WaitToComplete(this.Send(request), Configuration.SocketOptions.ConnectTimeoutMillis);
            if (response is AuthSuccessResponse)
            {
                //It is now authenticated
                return;
            }
            if (response is AuthChallengeResponse)
            {
                token = authenticator.EvaluateChallenge((response as AuthChallengeResponse).Token);
                if (token == null)
                {
                    // If we get a null response, then authentication has completed
                    //return without sending a further response back to the server.
                    return;
                }
                Authenticate(token, authenticator);
                return;
            }
            throw new ProtocolErrorException("Expected SASL response, obtained " + response.GetType().Name);
        }

        /// <summary>
        /// It callbacks all operations already sent / or to be written, that do not have a response.
        /// </summary>
        internal void CancelPending(Exception ex, SocketError? socketError = null)
        {
            //Multiple IO worker threads may been notifying that the socket is closing/in error
            lock (_cancelLock)
            {
                _isCanceled = true;
                if (_pendingOperations.Count == 0 && _writeQueue.Count == 0)
                {
                    return;
                }
                _logger.Info("Canceling pending operations " + _pendingOperations.Count + " and write queue " + _writeQueue.Count);
                if (ex == null)
                {
                    if (socketError != null)
                    {
                        ex = new SocketException((int)socketError.Value);
                    }
                    else
                    {
                        //It is closing
                        ex = new SocketException((int)SocketError.NotConnected);
                    }
                }
                if (_writeQueue.Count > 0)
                {
                    //Callback all the items in the write queue
                    OperationState state = null;
                    while (_writeQueue.TryDequeue(out state))
                    {
                        state.InvokeCallback(ex);
                    }
                }
                if (_pendingOperations.Count > 0)
                {
                    //Callback for every pending operation
                    foreach (var item in _pendingOperations)
                    {
                        item.Value.InvokeCallback(ex);
                    }
                    _pendingOperations.Clear();
                }
                if (_pendingWaitHandle != null)
                {
                    _pendingWaitHandle.Set();
                }
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

        private void EventHandler(Exception ex, AbstractResponse response)
        {
            if (!(response is EventResponse))
            {
                _logger.Error("Unexpected response type for event: " + response.GetType().Name);
                return;
            }
            if (this.CassandraEventResponse != null)
            {
                this.CassandraEventResponse(this, (response as EventResponse).CassandraEventArgs);
            }
        }

        /// <summary>
        /// Initializes the connection. Thread safe.
        /// </summary>
        /// <exception cref="SocketException">Throws a SocketException when the connection could not be established with the host</exception>
        /// <exception cref="AuthenticationException" />
        public virtual void Init()
        {
            //MAYBE: If really necessary, we can Wait on the BeginConnect result.
            //Cassandra supports up to 128 concurrent requests
            _freeOperations = new ConcurrentStack<byte>(Enumerable.Range(0, _maxConcurrentRequests).Select(s => (byte)s));
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

            var startupTask = Startup();
            TaskHelper.WaitToComplete(startupTask, _tcpSocket.Options.ConnectTimeoutMillis);
            if (startupTask.Result is AuthenticateResponse)
            {
                Authenticate();
            }
            else if (!(startupTask.Result is ReadyResponse))
            {
                throw new DriverInternalError("Expected READY or AUTHENTICATE, obtained " + startupTask.Result.GetType().Name);
            }
        }

        private void ReadHandler(byte[] buffer, int bytesReceived)
        {
            if (_isCanceled)
            {
                //All pending operations have been canceled, there is no point in reading from the wire.
                return;
            }
            //Parse the data received
            var streamIdAvailable = ReadParse(buffer, 0, bytesReceived);
            if (streamIdAvailable)
            {
                if (_pendingWaitHandle != null && _pendingOperations.Count == 0 && _writeQueue.Count == 0)
                {
                    _pendingWaitHandle.Set();
                }
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
                if (header.Opcode != EventResponse.OpCode)
                {
                    //Its a response to a previous request
                    state = _pendingOperations[header.StreamId];
                }
                else
                {
                    //Its an event
                    state = new OperationState();
                    state.Callback = EventHandler;
                }
                state.Header = header;
                _receivingOperation = state;
            }
            var countAdded = state.AppendBody(buffer, offset, count);

            if (state.IsBodyComplete)
            {
                _logger.Verbose("Read #" + state.Header.StreamId + " for Opcode " + state.Header.Opcode);
                //Stop reference it as the current receiving operation
                _receivingOperation = null;
                if (state.Header.Opcode != EventResponse.OpCode)
                {
                    //Remove from pending
                    _pendingOperations.TryRemove(state.Header.StreamId, out state);
                    //Release the streamId
                    _freeOperations.Push(state.Header.StreamId);
                }
                try
                {
                    var response = ReadParseResponse(state.Header, state.BodyStream);
                    state.InvokeCallback(null, response);
                }
                catch (Exception ex)
                {
                    state.InvokeCallback(ex);
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
        /// Sends a protocol STARTUP message
        /// </summary>
        private Task<AbstractResponse> Startup()
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
            var tcs = new TaskCompletionSource<AbstractResponse>();
            Send(request, tcs.TrySet);
            return tcs.Task;
        }

        /// <summary>
        /// Sends a new request if possible. If it is not possible it queues it up.
        /// </summary>
        public Task<AbstractResponse> Send(IRequest request)
        {
            var tcs = new TaskCompletionSource<AbstractResponse>();
            this.Send(request, (ex, r) => tcs.TrySet(ex, r));
            return tcs.Task;
        }

        /// <summary>
        /// Sends a new request if possible and executes the callback when the response is parsed. If it is not possible it queues it up.
        /// </summary>
        public void Send(IRequest request, Action<Exception, AbstractResponse> callback)
        {
            if (_isCanceled)
            {
                callback(new SocketException((int)SocketError.NotConnected), null);
            }
            //thread safe write queue
            var state = new OperationState
            {
                Request = request,
                Callback = callback
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
            byte streamId = 255;
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
            try
            {
                _logger.Verbose("Sending #" + streamId + " for " + state.Request.GetType().Name);
                var frameStream = state.Request.GetFrame(streamId, ProtocolVersion).Stream;
                _pendingOperations.AddOrUpdate(streamId, state, (k, oldValue) => state);
                //We will not use the request, stop reference it.
                state.Request = null;
                //Start sending it
                _tcpSocket.Write(frameStream);
            }
            catch (Exception ex)
            {
                //Prevent dead locking
                _canWriteNext = true;
                _logger.Error(ex);
                //The request was not written
                _pendingOperations.TryRemove(streamId, out state);
                _freeOperations.Push(streamId);
                throw;
            }
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

        internal WaitHandle WaitPending()
        {
            if (_pendingWaitHandle == null)
            {
                _pendingWaitHandle = new AutoResetEvent(false);
            }
            if (_pendingOperations.Count == 0 && _writeQueue.Count == 0)
            {
                _pendingWaitHandle.Set();
            }
            return _pendingWaitHandle;
        }
    }
}
