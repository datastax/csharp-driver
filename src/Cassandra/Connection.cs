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
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Net;
using System.Net.Sockets;
using System.Threading;
using System.Threading.Tasks;
using Cassandra.Tasks;
using Cassandra.Compression;

namespace Cassandra
{
    /// <summary>
    /// Represents a TCP connection to a Cassandra Node
    /// </summary>
    internal class Connection : IDisposable
    {
        // ReSharper disable once InconsistentNaming
        private static readonly Logger _logger = new Logger(typeof(Connection));
        private readonly TcpSocket _tcpSocket;
        private int _disposed;
        /// <summary>
        /// Determines that the connection canceled pending operations.
        /// It could be because its being closed or there was a socket error.
        /// </summary>
        private volatile bool _isCanceled;
        private readonly object _cancelLock = new object();
        private readonly Timer _idleTimer;
        private AutoResetEvent _pendingWaitHandle;
        private int _timedOutOperations;
        /// <summary>
        /// Stores the available stream ids.
        /// </summary>
        private ConcurrentStack<short> _freeOperations;
        /// <summary> Contains the requests that were sent through the wire and that hasn't been received yet.</summary>
        private ConcurrentDictionary<short, OperationState> _pendingOperations;
        /// <summary> It contains the requests that could not be written due to streamIds not available</summary>
        private ConcurrentQueue<OperationState> _writeQueue;
        private volatile OperationState _receivingOperation;
        /// <summary>
        /// Small buffer (less than 8 bytes) that is used when the next received message is smaller than 8 bytes, 
        /// and it is not possible to read the header.
        /// </summary>
        private volatile byte[] _minimalBuffer;
        private volatile string _keyspace;
        private readonly SemaphoreSlim _keyspaceSwitchSemaphore = new SemaphoreSlim(1);
        private volatile Task<bool> _keyspaceSwitchTask;
        private volatile byte _frameHeaderSize;
        /// <summary> TaskScheduler used to handle write tasks</summary>
        private readonly TaskScheduler _writeScheduler = new LimitedParallelismTaskScheduler(1);
        private int _isWriteQueueRuning;
        /// <summary>
        /// The event that represents a event RESPONSE from a Cassandra node
        /// </summary>
        public event CassandraEventHandler CassandraEventResponse;
        /// <summary>
        /// Event raised when there is an error when executing the request to prevent idle disconnects
        /// </summary>
        public event Action<Exception> OnIdleRequestException;
        /// <summary>
        /// Event that gets raised when a write has been completed. Testing purposes only.
        /// </summary>
        public event Action WriteCompleted;
        private const string IdleQuery = "SELECT key from system.local";
        private const long CoalescingThreshold = 8000;

        public IFrameCompressor Compressor { get; set; }

        public IPEndPoint Address
        {
            get { return _tcpSocket.IPEndPoint; }
        }

        /// <summary>
        /// Determines the amount of operations that are not finished.
        /// </summary>
        public int InFlight
        { 
            get { return _pendingOperations.Count; }
        }

        /// <summary>
        /// Gets the amount of operations that timed out and didn't get a response
        /// </summary>
        public int TimedOutOperations
        {
            get { return Thread.VolatileRead(ref _timedOutOperations); }
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
        /// Gets the current keyspace.
        /// </summary>
        public string Keyspace
        {
            get
            {
                return _keyspace;
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
            ProtocolVersion = protocolVersion;
            Configuration = configuration;
            _tcpSocket = new TcpSocket(endpoint, configuration.SocketOptions, configuration.ProtocolOptions.SslOptions);
            _idleTimer = new Timer(IdleTimeoutHandler, null, Timeout.Infinite, Timeout.Infinite);
        }

        /// <summary>
        /// Starts the authentication flow
        /// </summary>
        /// <exception cref="AuthenticationException" />
        private Task<AbstractResponse> Authenticate()
        {
            //Determine which authentication flow to use.
            //Check if its using a C* 1.2 with authentication patched version (like DSE 3.1)
            var isPatchedVersion = ProtocolVersion == 1 && !(Configuration.AuthProvider is NoneAuthProvider) && Configuration.AuthInfoProvider == null;
            if (ProtocolVersion < 2 && !isPatchedVersion)
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
                return Send(request)
                    .ContinueSync(response =>
                    {
                        if (!(response is ReadyResponse))
                        {
                            //If Cassandra replied with a auth response error
                            //The task already is faulted and the exception was already thrown.
                            throw new ProtocolErrorException("Expected SASL response, obtained " + response.GetType().Name);
                        }
                        return response;
                    });
            }
            //Use protocol v2+ authentication flow

            //NewAuthenticator will throw AuthenticationException when NoneAuthProvider
            var authenticator = Configuration.AuthProvider.NewAuthenticator(Address);

            var initialResponse = authenticator.InitialResponse() ?? new byte[0];
            return Authenticate(initialResponse, authenticator);
        }

        /// <exception cref="AuthenticationException" />
        private Task<AbstractResponse> Authenticate(byte[] token, IAuthenticator authenticator)
        {
            var request = new AuthResponseRequest(ProtocolVersion, token);
            return Send(request)
                .Then(response =>
                {
                    if (response is AuthSuccessResponse)
                    {
                        //It is now authenticated
                        return TaskHelper.ToTask(response);
                    }
                    if (response is AuthChallengeResponse)
                    {
                        token = authenticator.EvaluateChallenge(((AuthChallengeResponse)response).Token);
                        if (token == null)
                        {
                            // If we get a null response, then authentication has completed
                            //return without sending a further response back to the server.
                            return TaskHelper.ToTask(response);
                        }
                        return Authenticate(token, authenticator);
                    }
                    throw new ProtocolErrorException("Expected SASL response, obtained " + response.GetType().Name);
                });
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
                _logger.Info("Canceling pending operations {0} and write queue {1}", _pendingOperations.Count, _writeQueue.Count);
                if (socketError != null)
                {
                    _logger.Verbose("The socket status received was {0}", socketError.Value);
                }
                if (_pendingOperations.Count == 0 && _writeQueue.Count == 0)
                {
                    return;
                }
                if (ex == null || ex is ObjectDisposedException)
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
                    OperationState state;
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
            _idleTimer.Dispose();
            _tcpSocket.Dispose();
            _keyspaceSwitchSemaphore.Dispose();
        }

        private void EventHandler(Exception ex, AbstractResponse response)
        {
            if (!(response is EventResponse))
            {
                _logger.Error("Unexpected response type for event: " + response.GetType().Name);
                return;
            }
            if (CassandraEventResponse != null)
            {
                CassandraEventResponse(this, ((EventResponse) response).CassandraEventArgs);
            }
        }

        /// <summary>
        /// Gets executed once the idle timeout has passed
        /// </summary>
        private void IdleTimeoutHandler(object state)
        {
            //Ensure there are no more idle timeouts until the query finished sending
            if (_isCanceled)
            {
                if (!IsDisposed)
                {
                    //If it was not manually disposed
                    _logger.Warning("Can not issue an heartbeat request as connection is closed");
                    if (OnIdleRequestException != null)
                    {
                        OnIdleRequestException(new SocketException((int)SocketError.NotConnected));
                    }
                }
                return;
            }
            _logger.Verbose("Connection idling, issuing a Request to prevent idle disconnects");
            var request = new QueryRequest(ProtocolVersion, IdleQuery, false, QueryProtocolOptions.Default);
            Send(request, (ex, response) =>
            {
                if (ex == null)
                {
                    //The send succeeded
                    //There is a valid response but we don't care about the response
                    return;
                }
                _logger.Warning("Received heartbeat request exception " + ex.ToString());
                if (ex is SocketException && OnIdleRequestException != null)
                {
                    OnIdleRequestException(ex);
                }
            });
        }


        /// <summary>
        /// Initializes the connection.
        /// </summary>
        /// <exception cref="SocketException">Throws a SocketException when the connection could not be established with the host</exception>
        /// <exception cref="AuthenticationException" />
        /// <exception cref="UnsupportedProtocolVersionException"></exception>
        public Task<AbstractResponse> Open()
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

            //Init TcpSocket
            _tcpSocket.Init();
            _tcpSocket.Error += CancelPending;
            _tcpSocket.Closing += () => CancelPending(null, null);
            //Read and write event handlers are going to be invoked using IO Threads
            _tcpSocket.Read += ReadHandler;
            _tcpSocket.WriteCompleted += WriteCompletedHandler;
            return _tcpSocket
                .Connect()
                .Then(_ => Startup())
                .ContinueWith(t =>
                {
                    if (t.IsFaulted && t.Exception != null)
                    {
                        //Adapt the inner exception and rethrow
                        var ex = t.Exception.InnerException;
                        if (ex is ProtocolErrorException)
                        {
                            //As we are starting up, check for protocol version errors
                            //There is no other way than checking the error message from Cassandra
                            if (ex.Message.Contains("Invalid or unsupported protocol version"))
                            {
                                throw new UnsupportedProtocolVersionException(ProtocolVersion, ex);
                            }
                        }
                        if (ex is ServerErrorException && ProtocolVersion >= 3 && ex.Message.Contains("ProtocolException: Invalid or unsupported protocol version"))
                        {
                            //For some versions of Cassandra, the error is wrapped into a server error
                            //See CASSANDRA-9451
                            throw new UnsupportedProtocolVersionException(ProtocolVersion, ex);
                        }
                        throw ex;
                    }
                    return t.Result;
                }, TaskContinuationOptions.ExecuteSynchronously)
                .Then(response =>
                {
                    if (response is AuthenticateResponse)
                    {
                        return Authenticate();
                    }
                    if (response is ReadyResponse)
                    {
                        return TaskHelper.ToTask(response);
                    }
                    throw new DriverInternalError("Expected READY or AUTHENTICATE, obtained " + response.GetType().Name);
                });
        }

        /// <summary>
        /// Silently kill the connection, for testing purposes only
        /// </summary>
        internal void Kill()
        {
            _tcpSocket.Kill();
        }

        private void ReadHandler(byte[] buffer, int bytesReceived)
        {
            if (_isCanceled)
            {
                //All pending operations have been canceled, there is no point in reading from the wire.
                return;
            }
            //We are currently using an IO Thread
            //Parse the data received
            var streamIdAvailable = ReadParse(buffer, 0, bytesReceived);
            if (!streamIdAvailable)
            {
                return;
            }
            if (_pendingWaitHandle != null && _pendingOperations.Count == 0 && _writeQueue.Count == 0)
            {
                _pendingWaitHandle.Set();
            }
            //Process a next item in the queue if possible.
            //Maybe there are there items in the write queue that were waiting on a fresh streamId
            RunQueue(false);
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
            var state = _receivingOperation;
            if (state == null)
            {
                if (_minimalBuffer != null)
                {
                    buffer = Utils.JoinBuffers(_minimalBuffer, 0, _minimalBuffer.Length, buffer, offset, count);
                    offset = 0;
                    count = buffer.Length;
                }
                if (_frameHeaderSize == 0)
                {
                    //Read the first byte of the message to determine the version of the response
                    ProtocolVersion = FrameHeader.GetProtocolVersion(buffer);
                    _frameHeaderSize = FrameHeader.GetSize(ProtocolVersion);
                }
                var headerSize = _frameHeaderSize;
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
                    state = new OperationState(EventHandler);
                }
                state.Header = header;
                _receivingOperation = state;
            }
            var countAdded = state.AppendBody(buffer, offset, count);

            if (!state.IsBodyComplete)
            {
                //Nothing finished
                return false;
            }
            _logger.Verbose("Read #{0} for Opcode {1} from host {2}", state.Header.StreamId, state.Header.Opcode, Address);
            //Stop reference it as the current receiving operation
            _receivingOperation = null;
            if (state.Header.Opcode != EventResponse.OpCode)
            {
                RemoveFromPending(state.Header.StreamId);
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
            //There isn't enough data to read the whole frame.
            //It is already buffered, carry on.
        }

        private AbstractResponse ReadParseResponse(FrameHeader header, Stream body)
        {
            //Start at the first byte
            body.Position = 0;
            if ((header.Flags & FrameHeader.HeaderFlag.Compression) != 0)
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
            Send(request, tcs.TrySet);
            return tcs.Task;
        }

        /// <summary>
        /// Sends a new request if possible and executes the callback when the response is parsed. If it is not possible it queues it up.
        /// </summary>
        public OperationState Send(IRequest request, Action<Exception, AbstractResponse> callback)
        {
            if (_isCanceled)
            {
                callback(new SocketException((int)SocketError.NotConnected), null);
            }
            var state = new OperationState(callback)
            {
                Request = request
            };
            SendQueue(state);
            return state;
        }

        private void SendQueue(OperationState state)
        {
            //Store into queue
            //if its running => do nothing
            //if not running => start dequeueing
            _writeQueue.Enqueue(state);
            RunQueue(true);
        }

        private void RunQueue(bool useInlining)
        {
            var isAlreadyRunning = Interlocked.CompareExchange(ref _isWriteQueueRuning, 1, 0) == 1;
            if (isAlreadyRunning)
            {
                //there is another thread writing to the wire
                return;
            }
            if (useInlining)
            {
                //Use the current thread to start the write operation
                RunQueueAction();
                return;
            }
            //Start a new task using the TaskScheduler for writing
            Task.Factory.StartNew(RunQueueAction, CancellationToken.None, TaskCreationOptions.None, _writeScheduler);
        }

        private void RunQueueAction()
        {
            //Dequeue all items until threshold is passed
            long totalLength = 0;
            var buffers = new LinkedList<Stream>();
            while (totalLength < CoalescingThreshold)
            {
                OperationState state;
                if (!_writeQueue.TryDequeue(out state))
                {
                    //No more items in the write queue
                    break;
                }
                short streamId;
                if (!_freeOperations.TryPop(out streamId))
                {
                    //Queue it up for later.
                    _writeQueue.Enqueue(state);
                    //When receiving the next complete message, we can process it.
                    _logger.Info("Enqueued: {0}, if this message is recurrent consider configuring more connections per host or lower the pressure", _writeQueue.Count);
                    break;
                }
                _logger.Verbose("Sending #{0} for {1}", streamId, state.Request.GetType().Name);
                _pendingOperations.AddOrUpdate(streamId, state, (k, oldValue) => state);
                Stream frameStream;
                try
                {
                    frameStream = state.Request.GetFrame(streamId).Stream;
                    //Closure state variable
                    var delegateState = state;
                    if (Configuration.SocketOptions.ReadTimeoutMillis > 0 && Configuration.Timer != null)
                    {
                        state.Timeout = Configuration.Timer.NewTimeout(() => OnTimeout(delegateState), Configuration.SocketOptions.ReadTimeoutMillis);
                    }
                }
                catch (Exception ex)
                {
                    //There was an error while serializing or begin sending
                    _logger.Error(ex);
                    //The request was not written, clear it from pending operations
                    RemoveFromPending(streamId);
                    //Callback with the Exception
                    state.InvokeCallback(ex);
                    break;
                }
                //We will not use the request any more, stop reference it.
                state.Request = null;
                //Add it buffers to write
                buffers.AddLast(frameStream);
                totalLength += frameStream.Length;
            }
            if (totalLength == 0)
            {
                //nothing to write
                Interlocked.Exchange(ref _isWriteQueueRuning, 0);
                return;
            }
            //this can result in OOM
            var buffer = Utils.ReadAllBytes(buffers, totalLength);
            _tcpSocket.Write(buffer);
        }

        /// <summary>
        /// Removes an operation from pending and frees the stream id
        /// </summary>
        /// <param name="streamId"></param>
        private void RemoveFromPending(short streamId)
        {
            OperationState state;
            _pendingOperations.TryRemove(streamId, out state);
            //Set the streamId as available
            _freeOperations.Push(streamId);
        }

        /// <summary>
        /// Sets the keyspace of the connection.
        /// If the keyspace is different from the current value, it sends a Query request to change it
        /// </summary>
        public Task<bool> SetKeyspace(string value)
        {
            if (String.IsNullOrEmpty(value) || _keyspace == value)
            {
                //No need to switch
                return TaskHelper.Completed;
            }
            Task<bool> keyspaceSwitch;
            try
            {
                if (!_keyspaceSwitchSemaphore.Wait(0))
                {
                    //Could not enter semaphore
                    //It is very likely that the connection is already switching keyspace
                    keyspaceSwitch = _keyspaceSwitchTask;
                    if (keyspaceSwitch != null)
                    {
                        return keyspaceSwitch.Then(_ =>
                        {
                            //validate if the new keyspace is the expected
                            if (_keyspace != value)
                            {
                                //multiple concurrent switches to different keyspace
                                return SetKeyspace(value);
                            }
                            return TaskHelper.Completed;
                        });
                    }
                    _keyspaceSwitchSemaphore.Wait();
                }
            }
            catch (ObjectDisposedException)
            {
                //The semaphore was disposed, this connection is closed
                return TaskHelper.FromException<bool>(new SocketException((int) SocketError.NotConnected));
            }
            //Semaphore entered
            if (_keyspace == value)
            {
                //While waiting to enter the semaphore, the connection switched keyspace
                try
                {
                    _keyspaceSwitchSemaphore.Release();
                }
                catch (ObjectDisposedException)
                {
                    //this connection is now closed but the switch completed successfully
                }
                return TaskHelper.Completed;
            }
            var request = new QueryRequest(ProtocolVersion, string.Format("USE \"{0}\"", value), false, QueryProtocolOptions.Default);
            _logger.Info("Connection to host {0} switching to keyspace {1}", Address, value);
            keyspaceSwitch = _keyspaceSwitchTask = Send(request).ContinueSync(r =>
            {
                _keyspace = value;
                try
                {
                    _keyspaceSwitchSemaphore.Release();
                }
                catch (ObjectDisposedException)
                {
                    //this connection is now closed but the switch completed successfully
                }
                _keyspaceSwitchTask = null;
                return true;
            });
            return keyspaceSwitch;
        }

        private void OnTimeout(OperationState state)
        {
            var ex = new OperationTimedOutException(Address, Configuration.SocketOptions.ReadTimeoutMillis);
            //Invoke if it hasn't been invoked yet
            //Once the response is obtained, we decrement the timed out counter
            var timedout = state.SetTimedOut(ex, () => Interlocked.Decrement(ref _timedOutOperations) );
            if (!timedout)
            {
                //The response was obtained since the timer elapsed, move on
                return;
            }
            //Increase timed-out counter
            Interlocked.Increment(ref _timedOutOperations);
        }

        /// <summary>
        /// Method that gets executed when a write request has been completed.
        /// </summary>
        protected virtual void WriteCompletedHandler()
        {
            //This handler is invoked by IO threads
            //Make it quick
            if (WriteCompleted != null)
            {
                WriteCompleted();
            }
            //There is no need for synchronization here
            //Only 1 thread can be here at the same time.
            //Set the idle timeout to avoid idle disconnects
            var heartBeatInterval = Configuration.PoolingOptions != null ? Configuration.PoolingOptions.GetHeartBeatInterval() : null;
            if (heartBeatInterval > 0 && !_isCanceled)
            {
                try
                {
                    _idleTimer.Change(heartBeatInterval.Value, Timeout.Infinite);
                }
                catch (ObjectDisposedException)
                {
                    //This connection is being disposed
                    //Don't mind
                }
            }
            Interlocked.Exchange(ref _isWriteQueueRuning, 0);
            //Send the next request, if exists
            //It will use a new thread
            RunQueue(false);
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
