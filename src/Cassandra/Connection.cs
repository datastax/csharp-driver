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
using Cassandra.Requests;
using Cassandra.Responses;
using Cassandra.Serialization;
using Microsoft.IO;

namespace Cassandra
{
    /// <summary>
    /// Represents a TCP connection to a Cassandra Node
    /// </summary>
    internal class Connection : IDisposable
    {
        private static readonly Logger Logger = new Logger(typeof(Connection));
        private readonly Serializer _serializer;
        private readonly TcpSocket _tcpSocket;
        private long _disposed;
        /// <summary>
        /// Determines that the connection canceled pending operations.
        /// It could be because its being closed or there was a socket error.
        /// </summary>
        private volatile bool _isCanceled;
        private readonly object _cancelLock = new object();
        private readonly Timer _idleTimer;
        private AutoResetEvent _pendingWaitHandle;
        private long _timedOutOperations;
        /// <summary>
        /// Stores the available stream ids.
        /// </summary>
        private ConcurrentStack<short> _freeOperations;
        /// <summary> Contains the requests that were sent through the wire and that hasn't been received yet.</summary>
        private ConcurrentDictionary<short, OperationState> _pendingOperations;
        /// <summary> It contains the requests that could not be written due to streamIds not available</summary>
        private ConcurrentQueue<OperationState> _writeQueue;
        /// <summary>
        /// Small buffer (less than 8 bytes) that is used when the next received message is smaller than 8 bytes, 
        /// and it is not possible to read the header.
        /// </summary>
        private volatile byte[] _minimalBuffer;
        private volatile string _keyspace;
        private readonly SemaphoreSlim _keyspaceSwitchSemaphore = new SemaphoreSlim(1);
        private volatile Task<bool> _keyspaceSwitchTask;
        private volatile byte _frameHeaderSize;
        private MemoryStream _readStream;
        private int _isWriteQueueRuning;
        private long _inFlight;
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
        public virtual int InFlight
        { 
            get { return (int)Interlocked.Read(ref _inFlight); }
        }

        /// <summary>
        /// Gets the amount of operations that timed out and didn't get a response
        /// </summary>
        public virtual int TimedOutOperations
        {
            get { return (int)Interlocked.Read(ref _timedOutOperations); }
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
            get { return Interlocked.Read(ref _disposed) > 0L; }
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
                if (_serializer.ProtocolVersion < 3)
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

        public Configuration Configuration { get; set; }

        public Connection(Serializer serializer, IPEndPoint endpoint, Configuration configuration)
        {
            if (serializer == null)
            {
                throw new ArgumentNullException("serializer");
            }
            if (configuration == null)
            {
                throw new ArgumentNullException("configuration");
            }
            if (configuration.BufferPool == null)
            {
                throw new ArgumentNullException(null, "BufferPool can not be null");
            }
            _serializer = serializer;
            Configuration = configuration;
            _tcpSocket = new TcpSocket(endpoint, configuration.SocketOptions, configuration.ProtocolOptions.SslOptions);
            _idleTimer = new Timer(IdleTimeoutHandler, null, Timeout.Infinite, Timeout.Infinite);
        }

        /// <summary>
        /// Starts the authentication flow
        /// </summary>
        /// <param name="name">Authenticator name from server.</param>
        /// <exception cref="AuthenticationException" />
        private Task<Response> StartAuthenticationFlow(string name)
        {
            //Determine which authentication flow to use.
            //Check if its using a C* 1.2 with authentication patched version (like DSE 3.1)
            var protocolVersion = _serializer.ProtocolVersion;
            var isPatchedVersion = protocolVersion == 1 && !(Configuration.AuthProvider is NoneAuthProvider) && Configuration.AuthInfoProvider == null;
            if (protocolVersion < 2 && !isPatchedVersion)
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
                var request = new CredentialsRequest(credentials);
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
            if (Configuration.AuthProvider is IAuthProviderNamed)
            {
                //Provide name when required
                ((IAuthProviderNamed) Configuration.AuthProvider).SetName(name);
            }
            //NewAuthenticator will throw AuthenticationException when NoneAuthProvider
            var authenticator = Configuration.AuthProvider.NewAuthenticator(Address);

            var initialResponse = authenticator.InitialResponse() ?? new byte[0];
            return Authenticate(initialResponse, authenticator);
        }

        /// <exception cref="AuthenticationException" />
        private Task<Response> Authenticate(byte[] token, IAuthenticator authenticator)
        {
            var request = new AuthResponseRequest(token);
            return Send(request)
                .Then(response =>
                {
                    if (response is AuthSuccessResponse)
                    {
                        //It is now authenticated
                        // ReSharper disable once SuspiciousTypeConversion.Global
                        var disposableAuthenticator = authenticator as IDisposable;
                        if (disposableAuthenticator != null)
                        {
                            disposableAuthenticator.Dispose();
                        }
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
                Logger.Info("Canceling pending operations {0} and write queue {1}", Interlocked.Read(ref _inFlight), _writeQueue.Count);
                if (socketError != null)
                {
                    Logger.Verbose("The socket status received was {0}", socketError.Value);
                }
                if (_pendingOperations.IsEmpty && _writeQueue.IsEmpty)
                {
                    if (_pendingWaitHandle != null)
                    {
                        _pendingWaitHandle.Set();
                    }
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
                //Callback all the items in the write queue
                OperationState state;
                while (_writeQueue.TryDequeue(out state))
                {
                    state.InvokeCallback(ex);
                }
                //Callback for every pending operation
                foreach (var item in _pendingOperations)
                {
                    item.Value.InvokeCallback(ex);
                }
                _pendingOperations.Clear();
                Interlocked.Exchange(ref _inFlight, 0);
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
            var readStream = Interlocked.Exchange(ref _readStream, null);
            if (readStream != null)
            {
                readStream.Dispose();
            }
        }

        private void EventHandler(Exception ex, Response response)
        {
            if (!(response is EventResponse))
            {
                Logger.Error("Unexpected response type for event: " + response.GetType().Name);
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
                    Logger.Warning("Can not issue an heartbeat request as connection is closed");
                    if (OnIdleRequestException != null)
                    {
                        OnIdleRequestException(new SocketException((int)SocketError.NotConnected));
                    }
                }
                return;
            }
            Logger.Verbose("Connection idling, issuing a Request to prevent idle disconnects");
            var request = new QueryRequest(_serializer.ProtocolVersion, IdleQuery, false, QueryProtocolOptions.Default);
            Send(request, (ex, response) =>
            {
                if (ex == null)
                {
                    //The send succeeded
                    //There is a valid response but we don't care about the response
                    return;
                }
                Logger.Warning("Received heartbeat request exception " + ex.ToString());
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
        public Task<Response> Open()
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
#if !NETCORE
                Compressor = new LZ4Compressor();
#else
                return TaskHelper.FromException<Response>(new NotSupportedException("Lz4 compression not supported under .NETCore"));
#endif
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
            var protocolVersion = _serializer.ProtocolVersion;
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
                                throw new UnsupportedProtocolVersionException(protocolVersion, ex);
                            }
                        }
                        if (ex is ServerErrorException && protocolVersion >= 3 && ex.Message.Contains("ProtocolException: Invalid or unsupported protocol version"))
                        {
                            //For some versions of Cassandra, the error is wrapped into a server error
                            //See CASSANDRA-9451
                            throw new UnsupportedProtocolVersionException(protocolVersion, ex);
                        }
                        throw ex;
                    }
                    return t.Result;
                }, TaskContinuationOptions.ExecuteSynchronously)
                .Then(response =>
                {
                    if (response is AuthenticateResponse)
                    {
                        return StartAuthenticationFlow(((AuthenticateResponse)response).Authenticator);
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
            var streamIdAvailable = ReadParse(buffer, bytesReceived);
            if (!streamIdAvailable)
            {
                return;
            }
            if (_pendingWaitHandle != null && _pendingOperations.IsEmpty && _writeQueue.IsEmpty)
            {
                _pendingWaitHandle.Set();
            }
            //Process a next item in the queue if possible.
            //Maybe there are there items in the write queue that were waiting on a fresh streamId
            RunWriteQueue();
        }

        private volatile FrameHeader _receivingHeader;

        /// <summary>
        /// Parses the bytes received into a frame. Uses the internal operation state to do the callbacks.
        /// Returns true if a full operation (streamId) has been processed and there is one available.
        /// </summary>
        /// <returns>True if a full operation (streamId) has been processed.</returns>
        internal bool ReadParse(byte[] buffer, int length)
        {
            if (length <= 0)
            {
                return false;
            }
            byte protocolVersion;
            if (_frameHeaderSize == 0)
            {
                //The server replies the first message with the max protocol version supported
                protocolVersion = FrameHeader.GetProtocolVersion(buffer);
                _serializer.ProtocolVersion = protocolVersion;
                _frameHeaderSize = FrameHeader.GetSize(protocolVersion);
            }
            else
            {
                protocolVersion = _serializer.ProtocolVersion;
            }
            //Use _readStream to buffer between messages, under low pressure, it should be null most of the times
            var stream = Interlocked.Exchange(ref _readStream, null);
            var operationCallbacks = new LinkedList<Action<MemoryStream>>();
            var offset = 0;
            if (_minimalBuffer != null)
            {
                //use a negative offset to identify that there is a previous header buffer
                offset = -1 * _minimalBuffer.Length;
            }
            while (offset < length)
            {
                FrameHeader header;
                //The remaining body length to read from this buffer
                int remainingBodyLength;
                if (_receivingHeader == null)
                {
                    if (length - offset < _frameHeaderSize)
                    {
                        _minimalBuffer = offset >= 0 ?
                            Utils.SliceBuffer(buffer, offset, length - offset) :
                            //it should almost never be the case there isn't enough bytes to read the header more than once
                            // ReSharper disable once PossibleNullReferenceException
                            Utils.JoinBuffers(_minimalBuffer, 0, _minimalBuffer.Length, buffer, 0, length);
                        break;
                    }
                    if (offset >= 0)
                    {
                        header = FrameHeader.ParseResponseHeader(protocolVersion, buffer, offset);
                    }
                    else
                    {
                        header = FrameHeader.ParseResponseHeader(protocolVersion, _minimalBuffer, buffer);
                        _minimalBuffer = null;
                    }
                    Logger.Verbose("Received #{0} from {1}", header.StreamId, Address);
                    offset += _frameHeaderSize;
                    remainingBodyLength = header.BodyLength;
                }
                else
                {
                    header = _receivingHeader;
                    remainingBodyLength = header.BodyLength - (int) stream.Length;
                    _receivingHeader = null;
                }
                if (remainingBodyLength > length - offset)
                {
                    //the buffer does not contains the body for this frame, buffer for later
                    MemoryStream nextMessageStream;
                    if (operationCallbacks.Count == 0 && stream != null)
                    {
                        //There hasn't been any operations completed with this buffer
                        //And there is a previous stream: reuse it
                        nextMessageStream = stream;
                    }
                    else
                    {
                        nextMessageStream = Configuration.BufferPool.GetStream(typeof(Connection) + "/Read");
                    }
                    nextMessageStream.Write(buffer, offset, length - offset);
                    Interlocked.Exchange(ref _readStream, nextMessageStream);
                    _receivingHeader = header;
                    break;
                }
                stream = stream ?? Configuration.BufferPool.GetStream(typeof (Connection) + "/Read");
                OperationState state;
                if (header.Opcode != EventResponse.OpCode)
                {
                    state = RemoveFromPending(header.StreamId);
                }
                else
                {
                    //Its an event
                    state = new OperationState(EventHandler);
                }
                stream.Write(buffer, offset, remainingBodyLength);
                var callback = state.SetCompleted();
                operationCallbacks.AddLast(CreateResponseAction(header, callback));
                offset += remainingBodyLength;
            }
            return InvokeReadCallbacks(stream, operationCallbacks);
        }

        /// <summary>
        /// Returns an action that capture the parameters closure
        /// </summary>
        private Action<MemoryStream> CreateResponseAction(FrameHeader header, Action<Exception, Response> callback)
        {
            var compressor = Compressor;
            return delegate(MemoryStream stream)
            {
                Response response = null;
                Exception ex = null;
                var nextPosition = stream.Position + header.BodyLength;
                try
                {
                    Stream plainTextStream = stream;
                    if (header.Flags.HasFlag(FrameHeader.HeaderFlag.Compression))
                    {
                        plainTextStream = compressor.Decompress(new WrappedStream(stream, header.BodyLength));
                        plainTextStream.Position = 0;
                    }
                    response = FrameParser.Parse(new Frame(header, plainTextStream, _serializer));
                }
                catch (Exception catchedException)
                {
                    ex = catchedException;
                }
                if (response is ErrorResponse)
                {
                    //Create an exception from the response error
                    ex = ((ErrorResponse) response).Output.CreateException();
                    response = null;
                }
                //We must advance the position of the stream manually in case it was not correctly parsed
                stream.Position = nextPosition;
                callback(ex, response);
            };
        }

        /// <summary>
        /// Invokes the callbacks using the default TaskScheduler.
        /// </summary>
        /// <returns>Returns true if one or more callback has been invoked.</returns>
        private static bool InvokeReadCallbacks(MemoryStream stream, ICollection<Action<MemoryStream>> operationCallbacks)
        {
            if (operationCallbacks.Count == 0)
            {
                //Not enough data to read a frame
                return false;
            }
            //Invoke all callbacks using the default TaskScheduler
            Task.Factory.StartNew(() =>
            {
                stream.Position = 0;
                foreach (var cb in operationCallbacks)
                {
                    cb(stream);
                }
                stream.Dispose();
            }, CancellationToken.None, TaskCreationOptions.None, TaskScheduler.Default);
            return true;
        }

        /// <summary>
        /// Sends a protocol STARTUP message
        /// </summary>
        private Task<Response> Startup()
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
            return Send(new StartupRequest(startupOptions));
        }

        /// <summary>
        /// Sends a new request if possible. If it is not possible it queues it up.
        /// </summary>
        public Task<Response> Send(IRequest request)
        {
            var tcs = new TaskCompletionSource<Response>();
            Send(request, tcs.TrySet);
            return tcs.Task;
        }

        /// <summary>
        /// Sends a new request if possible and executes the callback when the response is parsed. If it is not possible it queues it up.
        /// </summary>
        public OperationState Send(IRequest request, Action<Exception, Response> callback, int timeoutMillis = Timeout.Infinite)
        {
            if (_isCanceled)
            {
                callback(new SocketException((int)SocketError.NotConnected), null);
                return null;
            }
            var state = new OperationState(callback)
            {
                Request = request,
                TimeoutMillis = timeoutMillis > 0 ? timeoutMillis : Configuration.SocketOptions.ReadTimeoutMillis
            };
            _writeQueue.Enqueue(state);
            RunWriteQueue();
            return state;
        }

        private void RunWriteQueue()
        {
            var isAlreadyRunning = Interlocked.CompareExchange(ref _isWriteQueueRuning, 1, 0) == 1;
            if (isAlreadyRunning)
            {
                //there is another thread writing to the wire
                return;
            }
            //Start a new task using the TaskScheduler for writing
            Task.Factory.StartNew(RunWriteQueueAction, CancellationToken.None, TaskCreationOptions.None, TaskScheduler.Default);
        }

        private void RunWriteQueueAction()
        {
            //Dequeue all items until threshold is passed
            long totalLength = 0;
            RecyclableMemoryStream stream = null;
            var streamIdsAvailable = true;
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
                    streamIdsAvailable = false;
                    //Queue it up for later.
                    _writeQueue.Enqueue(state);
                    //When receiving the next complete message, we can process it.
                    Logger.Info("Enqueued, no streamIds available. If this message is recurrent consider configuring more connections per host or lower the pressure");
                    break;
                }
                Logger.Verbose("Sending #{0} for {1} to {2}", streamId, state.Request.GetType().Name, Address);
                _pendingOperations.AddOrUpdate(streamId, state, (k, oldValue) => state);
                Interlocked.Increment(ref _inFlight);
                int frameLength;
                try
                {
                    //lazy initialize the stream
                    stream = stream ?? (RecyclableMemoryStream) Configuration.BufferPool.GetStream(GetType().Name + "/SendStream");
                    frameLength = state.Request.WriteFrame(streamId, stream, _serializer);
                    if (state.TimeoutMillis > 0 && Configuration.Timer != null)
                    {
                        state.Timeout = Configuration.Timer.NewTimeout(OnTimeout, streamId, state.TimeoutMillis);
                    }
                }
                catch (Exception ex)
                {
                    //There was an error while serializing or begin sending
                    Logger.Error(ex);
                    //The request was not written, clear it from pending operations
                    RemoveFromPending(streamId);
                    //Callback with the Exception
                    state.InvokeCallback(ex);
                    break;
                }
                //We will not use the request any more, stop reference it.
                state.Request = null;
                totalLength += frameLength;
            }
            if (totalLength == 0L)
            {
                //nothing to write
                Interlocked.Exchange(ref _isWriteQueueRuning, 0);
                if (streamIdsAvailable && !_writeQueue.IsEmpty)
                {
                    //The write queue is not empty
                    //An item was added to the queue but we were running: try to launch a new queue
                    RunWriteQueue();
                }
                if (stream != null)
                {
                    //The stream instance could be created if there was an exception while generating the frame
                    stream.Dispose();
                }
                return;
            }
            //Write and close the stream when flushed
            // ReSharper disable once PossibleNullReferenceException : if totalLength > 0 the stream is initialized
            _tcpSocket.Write(stream, () => stream.Dispose());
        }

        /// <summary>
        /// Removes an operation from pending and frees the stream id
        /// </summary>
        /// <param name="streamId"></param>
        internal protected virtual OperationState RemoveFromPending(short streamId)
        {
            OperationState state;
            if (_pendingOperations.TryRemove(streamId, out state))
            {
                Interlocked.Decrement(ref _inFlight);
            }
            //Set the streamId as available
            _freeOperations.Push(streamId);
            return state;
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
            var request = new QueryRequest(_serializer.ProtocolVersion, string.Format("USE \"{0}\"", value), false, QueryProtocolOptions.Default);
            Logger.Info("Connection to host {0} switching to keyspace {1}", Address, value);
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

        private void OnTimeout(object stateObj)
        {
            var streamId = (short)stateObj;
            OperationState state;
            if (!_pendingOperations.TryGetValue(streamId, out state))
            {
                return;
            }
            var ex = new OperationTimedOutException(Address, state.TimeoutMillis);
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
            RunWriteQueue();
        }

        internal WaitHandle WaitPending()
        {
            if (_pendingWaitHandle == null)
            {
                _pendingWaitHandle = new AutoResetEvent(false);
            }
            if (_pendingOperations.IsEmpty && _writeQueue.IsEmpty)
            {
                _pendingWaitHandle.Set();
            }
            return _pendingWaitHandle;
        }
    }
}
