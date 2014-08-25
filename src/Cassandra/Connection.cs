using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Net;
using System.Net.Sockets;
using System.Threading;
using System.Threading.Tasks;

namespace Cassandra
{
    /// <summary>
    /// Represents a TCP connection to a Cassandra Node
    /// </summary>
    internal class Connection : IDisposable
    {
        private static readonly Logger _logger = new Logger(typeof(Connection));
        private readonly TcpSocket _tcpSocket;
        private int _disposed;
        /// <summary>
        /// Determines that the connection canceled pending operations.
        /// It could be because its being closed or there was a socket error.
        /// </summary>
        private volatile bool _isCanceled;
        private readonly object _cancelLock = new object();
        private AutoResetEvent _pendingWaitHandle;
        /// <summary>
        /// Stores the available stream ids.
        /// </summary>
        private ConcurrentStack<short> _freeOperations;
        /// <summary>
        /// Contains the requests that were sent through the wire and that hasn't been received yet.
        /// </summary>
        private ConcurrentDictionary<short, OperationState> _pendingOperations;
        /// <summary>
        /// It determines if the write queue can process the next (if it is not in-flight).
        /// It has to be volatile as it can not be cached by the thread.
        /// </summary>
        private volatile bool _canWriteNext = true;
        /// <summary>
        /// Its for processing the next item in the write queue.
        /// It can not be replaced by a Interlocked Increment as it must allow rollbacks (when there are no stream ids left).
        /// </summary>
        private readonly object _writeQueueLock = new object();
        private ConcurrentQueue<OperationState> _writeQueue;
        private OperationState _receivingOperation;
        /// <summary>
        /// Small buffer (less than 8 bytes) that is used when the next received message is smaller than 8 bytes, 
        /// and it is not possible to read the header.
        /// </summary>
        private byte[] _minimalBuffer;
        private volatile string _keyspace;
        private readonly object _keyspaceLock = new object();
        /// <summary>
        /// The event that represents a event RESPONSE from a Cassandra node
        /// </summary>
        public event CassandraEventHandler CassandraEventResponse;

        public IFrameCompressor Compressor { get; set; }

        public IPEndPoint Address
        {
            get
            {
                return _tcpSocket.IPEndPoint;
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
        /// Determine if the Connection is closed
        /// </summary>
        public bool IsClosed
        {
            //if the connection attempted to cancel pending operations
            get { return _isCanceled; }
        }

        /// <summary>
        /// Determine if the Connection has been explicitly disposed
        /// </summary>
        public bool IsDisposed
        {
            get { return Thread.VolatileRead(ref _disposed) > 0; }
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
                    _logger.Info("Connection to host " + Address + " switching to keyspace " + value);
                    this._keyspace = value;
                    var timeout = Configuration.SocketOptions.ConnectTimeoutMillis;
                    var request = new QueryRequest(ProtocolVersion, String.Format("USE \"{0}\"", value), false, QueryProtocolOptions.Default);
                    TaskHelper.WaitToComplete(this.Send(request), timeout);
                }
            }
        }

        /// <summary>
        /// Gets the amount of concurrent requests depending on the protocol version
        /// </summary>
        public int MaxConcurrentRequests
        {
            get
            {
                if (ProtocolVersion < 3)
                {
                    return 128;
                }
                //Protocol 3 supports up to 32K concurrent request without waiting a response
                //Allowing larger amounts of concurrent requests will cause large memory consumption
                //Limit to 2K per connection sounds reasonable.
                return 2048;
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
            //Determine which authentication flow to use.
            //Check if its using a C* 1.2 with authentication patched version (like DSE 3.1)
            var isPatchedVersion = ProtocolVersion == 1 && !(Configuration.AuthProvider is NoneAuthProvider) && Configuration.AuthInfoProvider == null;
            if (ProtocolVersion >= 2 || isPatchedVersion)
            {
                //Use protocol v2+ authentication flow

                //NewAuthenticator will throw AuthenticationException when NoneAuthProvider
                var authenticator = Configuration.AuthProvider.NewAuthenticator(Address);

                var initialResponse = authenticator.InitialResponse() ?? new byte[0];
                Authenticate(initialResponse, authenticator);
            }
            else
            {
                //Use protocol v1 authentication flow
                if (Configuration.AuthInfoProvider == null)
                {
                    throw new AuthenticationException(
                        String.Format("Host {0} requires authentication, but no credentials provided in Cluster configuration", Address),
                        Address);
                }
                var credentialsProvider = Configuration.AuthInfoProvider;
                var credentials = credentialsProvider.GetAuthInfos(Address);
                var request = new CredentialsRequest(ProtocolVersion, credentials);
                var response = TaskHelper.WaitToComplete(this.Send(request), Configuration.SocketOptions.ConnectTimeoutMillis);
                //If Cassandra replied with a auth response error
                //The task already is faulted and the exception was already thrown.
                if (response is ReadyResponse)
                {
                    return;
                }
                throw new ProtocolErrorException("Expected SASL response, obtained " + response.GetType().Name);
            }
        }

        /// <exception cref="AuthenticationException" />
        private void Authenticate(byte[] token, IAuthenticator authenticator)
        {
            var request = new AuthResponseRequest(this.ProtocolVersion, token);
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
                if (socketError != null)
                {
                    _logger.Verbose("Socket error " + socketError.Value);   
                }
                _logger.Info("Canceling pending operations " + _pendingOperations.Count + " and write queue " + _writeQueue.Count);
                if (_pendingOperations.Count == 0 && _writeQueue.Count == 0)
                {
                    return;
                }
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
            if (Interlocked.Increment(ref _disposed) != 1)
            {
                //Only dispose once
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
        /// <exception cref="UnsupportedProtocolVersionException"></exception>
        public virtual void Init()
        {
            _freeOperations = new ConcurrentStack<short>(Enumerable.Range(0, MaxConcurrentRequests).Select(s => (short)s).Reverse());
            _pendingOperations = new ConcurrentDictionary<short, OperationState>();
            _writeQueue = new ConcurrentQueue<OperationState>();

            if (Options.CustomCompressor != null)
            {
                Compressor = Options.CustomCompressor;
            }
            else if (Options.Compression == CompressionType.LZ4)
            {
                Compressor = new LZ4Compressor();
            }
            else if (Options.Compression == CompressionType.Snappy)
            {
                Compressor = new SnappyCompressor();
            }

            //MAYBE: If really necessary, we can Wait on the BeginConnect result.
            //Init TcpSocket
            _tcpSocket.Init();
            _tcpSocket.Error += CancelPending;
            _tcpSocket.Closing += () => CancelPending(null, null);
            _tcpSocket.Read += ReadHandler;
            _tcpSocket.WriteCompleted += WriteCompletedHandler;
            _tcpSocket.Connect();

            var startupTask = Startup();
            try
            {
                TaskHelper.WaitToComplete(startupTask, _tcpSocket.Options.ConnectTimeoutMillis);
            }
            catch (ProtocolErrorException ex)
            {
                //As we are starting up, check for protocol version errors
                //There is no other way than checking the error message from Cassandra
                if (ex.Message.Contains("Invalid or unsupported protocol version"))
                {
                    throw new UnsupportedProtocolVersionException(ProtocolVersion, ex);
                }
                throw;
            }
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
                var headerSize = FrameHeader.GetSize(ProtocolVersion);
                if (count < headerSize)
                {
                    //There is not enough data to read the header
                    _minimalBuffer = Utils.SliceBuffer(buffer, offset, count);
                    return false;
                }
                _minimalBuffer = null;
                var header = FrameHeader.ParseResponseHeader(ProtocolVersion, buffer, offset);
                if (!header.IsValidResponse())
                {
                    _logger.Error("Not a response header");
                }
                offset += headerSize;
                count -= headerSize;
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
            var request = new StartupRequest(ProtocolVersion, startupOptions);
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
            this.Send(request, tcs.TrySet);
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
            short streamId = -1;
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
                    _logger.Info("Enqueued: " + _writeQueue.Count + ", if this message is recurrent consider configuring more connections per host or lower the pressure");
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
                var frameStream = state.Request.GetFrame(streamId).Stream;
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
            if (!_canWriteNext)
            {
                return;
            }
            OperationState state;
            if (_writeQueue.TryDequeue(out state))
            {
                SendQueueProcess(state);
            }
        }

        /// <summary>
        /// Method that gets executed when a write request has been completed.
        /// </summary>
        protected virtual void WriteCompletedHandler()
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
