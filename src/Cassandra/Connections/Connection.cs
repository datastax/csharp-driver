//
//      Copyright (C) DataStax Inc.
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
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Net;
using System.Net.Sockets;
using System.Threading;
using System.Threading.Tasks;
using Cassandra.Compression;
using Cassandra.Metrics;
using Cassandra.Observers.Abstractions;
using Cassandra.Requests;
using Cassandra.Responses;
using Cassandra.Serialization;
using Cassandra.Tasks;
using Microsoft.IO;

namespace Cassandra.Connections
{
    /// <inheritdoc />
    internal class Connection : IConnection
    {
        private const int WriteStateInit = 0;
        private const int WriteStateRunning = 1;
        private const int WriteStateClosed = 2;
        private const string StreamReadTag = nameof(Connection) + "/Read";
        private const string StreamWriteTag = nameof(Connection) + "/Write";

        private static readonly Logger Logger = new Logger(typeof(Connection));

        private readonly IStartupRequestFactory _startupRequestFactory;
        private readonly ITcpSocket _tcpSocket;
        private long _disposed;

        /// <summary>
        /// Determines that the connection canceled pending operations.
        /// It could be because its being closed or there was a socket error.
        /// </summary>
        private volatile bool _isCanceled;

        private readonly Timer _idleTimer;
        private long _timedOutOperations;

        /// <summary>
        /// Stores the available stream ids.
        /// </summary>
        private ConcurrentStack<short> _freeOperations;

        /// <summary> Contains the requests that were sent through the wire and that hasn't been received yet.</summary>
        private ConcurrentDictionary<short, OperationState> _pendingOperations;

        /// <summary> It contains the requests that have a streamid and are waiting to be written</summary>
        private ConcurrentQueue<OperationState> _writeQueue;

        private volatile string _keyspace;
        private TaskCompletionSource<bool> _keyspaceSwitchTcs;

        /// <summary>
        /// Small buffer (less than 8 bytes) that is used when the next received message is smaller than 8 bytes,
        /// and it is not possible to read the header.
        /// </summary>
        private byte[] _minHeaderBuffer;

        private ISerializer _serializer;
        private int _frameHeaderSize;
        private MemoryStream _readStream;
        private FrameHeader _receivingHeader;
        private int _writeState = Connection.WriteStateInit;
        private int _inFlight;
        private readonly IConnectionObserver _connectionObserver;
        private readonly bool _timerEnabled;
        private readonly int _heartBeatInterval;

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

        /// <summary>
        /// Event that gets raised the connection is being closed.
        /// </summary>
        public event Action<IConnection> Closing;

        private const string IdleQuery = "SELECT key from system.local";
        private const long CoalescingThreshold = 8000;

        public ISerializer Serializer => Volatile.Read(ref _serializer);

        public IFrameCompressor Compressor { get; set; }

        public IConnectionEndPoint EndPoint => _tcpSocket.EndPoint;

        public IPEndPoint LocalAddress => _tcpSocket.GetLocalIpEndPoint();

        public int WriteQueueLength => _writeQueue.Count;

        public int PendingOperationsMapLength => _pendingOperations.Count;

        /// <summary>
        /// Determines the amount of operations that are not finished.
        /// </summary>
        public virtual int InFlight => Volatile.Read(ref _inFlight);

        /// <summary>
        /// Determines if there isn't any operations pending to be written or inflight.
        /// </summary>
        public virtual bool HasPendingOperations
        {
            get { return InFlight > 0 || !_writeQueue.IsEmpty; }
        }

        /// <summary>
        /// Gets the amount of operations that timed out and didn't get a response
        /// </summary>
        public virtual int TimedOutOperations
        {
            get { return (int)Interlocked.Read(ref _timedOutOperations); }
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

        public ProtocolOptions Options => Configuration.ProtocolOptions;

        public Configuration Configuration { get; set; }

        internal Connection(
            ISerializer serializer,
            IConnectionEndPoint endPoint,
            Configuration configuration,
            IStartupRequestFactory startupRequestFactory,
            IConnectionObserver connectionObserver)
        {
            _serializer = serializer ?? throw new ArgumentNullException(nameof(serializer));
            Configuration = configuration ?? throw new ArgumentNullException(nameof(configuration));
            _startupRequestFactory = startupRequestFactory ?? throw new ArgumentNullException(nameof(startupRequestFactory));
            _heartBeatInterval = configuration.GetHeartBeatInterval() ?? 0;
            _tcpSocket = new TcpSocket(endPoint, configuration.SocketOptions, configuration.ProtocolOptions.SslOptions);
            _idleTimer = new Timer(IdleTimeoutHandler, null, Timeout.Infinite, Timeout.Infinite);
            _connectionObserver = connectionObserver;
            _timerEnabled = configuration.MetricsEnabled
                            && configuration.MetricsOptions.EnabledNodeMetrics.Contains(NodeMetric.Timers.CqlMessages);
        }

        private void IncrementInFlight()
        {
            Interlocked.Increment(ref _inFlight);
        }

        private void DecrementInFlight()
        {
            Interlocked.Decrement(ref _inFlight);
        }

        /// <summary>
        /// Gets the amount of concurrent requests depending on the protocol version
        /// </summary>
        public int GetMaxConcurrentRequests(ISerializer serializer)
        {
            if (!serializer.ProtocolVersion.Uses2BytesStreamIds())
            {
                return 128;
            }
            //Protocol 3 supports up to 32K concurrent request without waiting a response
            //Allowing larger amounts of concurrent requests will cause large memory consumption
            //Limit to 2K per connection sounds reasonable.
            return 2048;
        }

        /// <summary>
        /// Starts the authentication flow
        /// </summary>
        /// <param name="name">Authenticator name from server.</param>
        /// <exception cref="AuthenticationException" />
        private async Task<Response> StartAuthenticationFlow(string name)
        {
            //Determine which authentication flow to use.
            //Check if its using a C* 1.2 with authentication patched version (like DSE 3.1)
            var protocolVersion = Serializer.ProtocolVersion;
            var isPatchedVersion = protocolVersion == ProtocolVersion.V1 &&
                !(Configuration.AuthProvider is NoneAuthProvider) && Configuration.AuthInfoProvider == null;
            if (protocolVersion == ProtocolVersion.V1 && !isPatchedVersion)
            {
                //Use protocol v1 authentication flow
                if (Configuration.AuthInfoProvider == null)
                {
                    throw new AuthenticationException(
                        $"Host {EndPoint.EndpointFriendlyName} requires authentication, but no credentials provided in Cluster configuration",
                        EndPoint.GetHostIpEndPointWithFallback());
                }
                var credentialsProvider = Configuration.AuthInfoProvider;
                var credentials = credentialsProvider.GetAuthInfos(EndPoint.GetHostIpEndPointWithFallback());
                var request = new CredentialsRequest(credentials);
                var response = await Send(request).ConfigureAwait(false);
                if (!(response is ReadyResponse))
                {
                    //If Cassandra replied with a auth response error
                    //The task already is faulted and the exception was already thrown.
                    throw new ProtocolErrorException("Expected SASL response, obtained " + response.GetType().Name);
                }
                return response;
            }
            //Use protocol v2+ authentication flow
            if (Configuration.AuthProvider is IAuthProviderNamed)
            {
                //Provide name when required
                ((IAuthProviderNamed)Configuration.AuthProvider).SetName(name);
            }
            //NewAuthenticator will throw AuthenticationException when NoneAuthProvider
            var authenticator = Configuration.AuthProvider.NewAuthenticator(EndPoint.GetHostIpEndPointWithFallback());

            var initialResponse = authenticator.InitialResponse() ?? new byte[0];
            return await Authenticate(initialResponse, authenticator).ConfigureAwait(false);
        }

        /// <exception cref="AuthenticationException" />
        private async Task<Response> Authenticate(byte[] token, IAuthenticator authenticator)
        {
            var request = new AuthResponseRequest(token);
            var response = await Send(request).ConfigureAwait(false);

            if (response is AuthSuccessResponse)
            {
                // It is now authenticated, dispose Authenticator if it implements IDisposable()
                // ReSharper disable once SuspiciousTypeConversion.Global
                var disposableAuthenticator = authenticator as IDisposable;
                if (disposableAuthenticator != null)
                {
                    disposableAuthenticator.Dispose();
                }
                return response;
            }
            if (response is AuthChallengeResponse)
            {
                token = authenticator.EvaluateChallenge(((AuthChallengeResponse)response).Token);
                if (token == null)
                {
                    // If we get a null response, then authentication has completed
                    // return without sending a further response back to the server.
                    return response;
                }
                return await Authenticate(token, authenticator).ConfigureAwait(false);
            }
            throw new ProtocolErrorException("Expected SASL response, obtained " + response.GetType().Name);
        }

        /// <summary>
        /// It callbacks all operations already sent / or to be written, that do not have a response.
        /// Invoked from an IO Thread or a pool thread
        /// </summary>
        internal void CancelPending(Exception ex, SocketError? socketError = null)
        {
            _isCanceled = true;
            var wasClosed = Interlocked.Exchange(ref _writeState, Connection.WriteStateClosed) == Connection.WriteStateClosed;
            if (!wasClosed)
            {
                Closing?.Invoke(this);

                Connection.Logger.Info("Cancelling in Connection {0}, {1} pending operations and write queue {2}", EndPoint.EndpointFriendlyName,
                    InFlight, _writeQueue.Count);

                if (socketError != null)
                {
                    Connection.Logger.Verbose("The socket status received was {0}", socketError.Value);
                }
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
            // Dequeue all the items in the write queue
            var ops = new LinkedList<OperationState>();
            OperationState state;
            while (_writeQueue.TryDequeue(out state))
            {
                ops.AddLast(state);
            }
            // Remove every pending operation
            while (!_pendingOperations.IsEmpty)
            {
                Interlocked.MemoryBarrier();
                // Remove using a snapshot of the keys
                var keys = _pendingOperations.Keys.ToArray();
                foreach (var key in keys)
                {
                    if (_pendingOperations.TryRemove(key, out state))
                    {
                        ops.AddLast(state);
                    }
                }
            }
            Interlocked.MemoryBarrier();
            OperationState.CallbackMultiple(ops, RequestError.CreateClientError(ex, false), GetTimestamp());
            Interlocked.Exchange(ref _inFlight, 0);
        }

        public virtual void Dispose()
        {
            if (Interlocked.Increment(ref _disposed) != 1)
            {
                //Only dispose once
                return;
            }

            Connection.Logger.Verbose("Disposing Connection #{0} to {1}.", GetHashCode(), EndPoint.EndpointFriendlyName);

            _idleTimer.Dispose();
            _tcpSocket.Dispose();
            var readStream = Interlocked.Exchange(ref _readStream, null);
            if (readStream != null)
            {
                readStream.Dispose();
            }
        }

        private void EventHandler(IRequestError error, Response response, long timestamp)
        {
            if (!(response is EventResponse))
            {
                Connection.Logger.Error("Unexpected response type for event: " + response.GetType().Name);
                return;
            }

            CassandraEventResponse?.Invoke(this, ((EventResponse)response).CassandraEventArgs);
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
                    Connection.Logger.Warning("Can not issue an heartbeat request as connection is closed");
                    OnIdleRequestException?.Invoke(new SocketException((int)SocketError.NotConnected));
                }
                return;
            }
            Connection.Logger.Verbose("Connection idling, issuing a Request to prevent idle disconnects");
            var request = new OptionsRequest();
            Send(request, (error, response) =>
            {
                if (error?.Exception == null)
                {
                    //The send succeeded
                    //There is a valid response but we don't care about the response
                    return;
                }
                Connection.Logger.Warning("Received heartbeat request exception " + error.Exception.ToString());
                if (error.Exception is SocketException)
                {
                    OnIdleRequestException?.Invoke(error.Exception);
                }
            });
        }

        /// <summary>
        /// Initializes the connection.
        /// </summary>
        /// <exception cref="SocketException">Throws a SocketException when the connection could not be established with the host</exception>
        /// <exception cref="AuthenticationException" />
        /// <exception cref="UnsupportedProtocolVersionException"></exception>
        public async Task<Response> Open()
        {
            try
            {
                var response = await DoOpen().ConfigureAwait(false);
                Connection.Logger.Verbose("Opened Connection #{0} to {1}.", GetHashCode(), EndPoint.EndpointFriendlyName);
                return response;
            }
            catch (Exception exception)
            {
                _connectionObserver.OnErrorOnOpen(exception);
                throw;
            }
        }

        /// <summary>
        /// Initializes the connection.
        /// </summary>
        /// <exception cref="SocketException">Throws a SocketException when the connection could not be established with the host</exception>
        /// <exception cref="AuthenticationException" />
        /// <exception cref="UnsupportedProtocolVersionException"></exception>
        public async Task<Response> DoOpen()
        {
            _freeOperations = new ConcurrentStack<short>(Enumerable.Range(0, GetMaxConcurrentRequests(Serializer)).Select(s => (short)s).Reverse());
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
            _tcpSocket.Closing += () => CancelPending(null);
            //Read and write event handlers are going to be invoked using IO Threads
            _tcpSocket.Read += ReadHandler;
            _tcpSocket.WriteCompleted += WriteCompletedHandler;
            var protocolVersion = Serializer.ProtocolVersion;
            await _tcpSocket.Connect().ConfigureAwait(false);
            Response response;
            try
            {
                response = await Startup().ConfigureAwait(false);
            }
            catch (ProtocolErrorException ex)
            {
                // As we are starting up, check for protocol version errors.
                // There is no other way than checking the error message from Cassandra
                if (ex.Message.Contains("Invalid or unsupported protocol version"))
                {
                    throw new UnsupportedProtocolVersionException(protocolVersion, Serializer.ProtocolVersion, ex);
                }
                throw;
            }
            if (response is AuthenticateResponse)
            {
                return await StartAuthenticationFlow(((AuthenticateResponse)response).Authenticator)
                    .ConfigureAwait(false);
            }
            if (response is ReadyResponse)
            {
                return response;
            }
            throw new DriverInternalError("Expected READY or AUTHENTICATE, obtained " + response.GetType().Name);
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

            _connectionObserver.OnBytesReceived(bytesReceived);
            //We are currently using an IO Thread
            //Parse the data received
            var streamIdAvailable = ReadParse(buffer, bytesReceived);
            if (!streamIdAvailable)
            {
                return;
            }
            //Process a next item in the queue if possible.
            //Maybe there are there items in the write queue that were waiting on a fresh streamId
            RunWriteQueue();
        }

        /// <summary>
        /// Deserializes each frame header and copies the body bytes into a single buffer.
        /// </summary>
        /// <returns>True if a full operation (streamId) has been processed.</returns>
        internal bool ReadParse(byte[] buffer, int length)
        {
            if (length <= 0)
            {
                return false;
            }

            // Check if protocol version has already been determined (first message)
            ProtocolVersion protocolVersion;
            var headerLength = Volatile.Read(ref _frameHeaderSize);
            var serializer = Volatile.Read(ref _serializer);
            if (headerLength == 0)
            {
                // The server replies the first message with the max protocol version supported
                protocolVersion = FrameHeader.GetProtocolVersion(buffer);
                serializer = serializer.CloneWithProtocolVersion(protocolVersion);
                headerLength = protocolVersion.GetHeaderSize();

                Volatile.Write(ref _serializer, serializer);
                Volatile.Write(ref _frameHeaderSize, headerLength);
                _frameHeaderSize = headerLength;
            }
            else
            {
                protocolVersion = serializer.ProtocolVersion;
            }

            // Use _readStream to buffer between messages, when the body is not contained in a single read call
            var stream = Interlocked.Exchange(ref _readStream, null);
            var previousHeader = Interlocked.Exchange(ref _receivingHeader, null);
            if (previousHeader != null && stream == null)
            {
                // This connection has been disposed
                return false;
            }

            var operationCallbacks = new LinkedList<Action<MemoryStream, long>>();
            var offset = 0;
            while (offset < length)
            {
                FrameHeader header;
                int remainingBodyLength;

                // check if header has not been read yet
                if (previousHeader == null)
                {
                    header = ReadHeader(buffer, ref offset, length, headerLength, protocolVersion);
                    if (header == null)
                    {
                        // There aren't enough bytes to read the header
                        break;
                    }

                    Connection.Logger.Verbose("Received #{0} from {1}", header.StreamId, EndPoint.EndpointFriendlyName);
                    remainingBodyLength = header.BodyLength;
                }
                else
                {
                    header = previousHeader;
                    previousHeader = null;
                    remainingBodyLength = header.BodyLength - (int)stream.Length;
                }

                if (remainingBodyLength > length - offset)
                {
                    // The buffer does not contains the body for the current frame, store it for later
                    StoreReadState(header, stream, buffer, offset, length, operationCallbacks.Count > 0);
                    break;
                }

                // Get read stream
                stream = stream ?? Configuration.BufferPool.GetStream(Connection.StreamReadTag);

                // Get callback and operation state
                Action<IRequestError, Response, long> callback;
                ResultMetadata resultMetadata = null;
                if (header.Opcode == EventResponse.OpCode)
                {
                    callback = EventHandler;
                }
                else
                {
                    var state = RemoveFromPending(header.StreamId);

                    // State can be null when the Connection is being closed concurrently
                    // The original callback is being called with an error, use a Noop here
                    if (state == null)
                    {
                        callback = OperationState.Noop;
                    }
                    else
                    {
                        callback = state.SetCompleted();
                        resultMetadata = state.ResultMetadata;
                    }
                }

                // Write to read stream
                stream.Write(buffer, offset, remainingBodyLength);

                // Add callback with deserialize from stream
                operationCallbacks.AddLast(CreateResponseAction(resultMetadata, serializer, header, callback));

                offset += remainingBodyLength;
            }

            // Invoke callbacks with read stream
            return Connection.InvokeReadCallbacks(stream, operationCallbacks, GetTimestamp());
        }

        /// <summary>
        /// Reads the header from the buffer, using previous
        /// </summary>
        private FrameHeader ReadHeader(byte[] buffer, ref int offset, int length, int headerLength,
                                       ProtocolVersion version)
        {
            if (offset == 0)
            {
                var previousHeaderBuffer = Interlocked.Exchange(ref _minHeaderBuffer, null);
                if (previousHeaderBuffer != null)
                {
                    if (previousHeaderBuffer.Length + length < headerLength)
                    {
                        // Unlikely scenario where there were a few bytes for a header buffer and the new bytes are
                        // not enough to complete the header
                        Volatile.Write(ref _minHeaderBuffer,
                            Utils.JoinBuffers(previousHeaderBuffer, 0, previousHeaderBuffer.Length, buffer, 0, length));
                        return null;
                    }
                    offset += headerLength - previousHeaderBuffer.Length;
                    // Use the previous and the current buffer to build the header
                    return FrameHeader.ParseResponseHeader(version, previousHeaderBuffer, buffer);
                }
            }
            if (length - offset < headerLength)
            {
                // There aren't enough bytes in the current buffer to read the header, store it for later
                Volatile.Write(ref _minHeaderBuffer, Utils.SliceBuffer(buffer, offset, length - offset));
                return null;
            }
            // The header is contained in the current buffer
            var header = FrameHeader.ParseResponseHeader(version, buffer, offset);
            offset += headerLength;
            return header;
        }

        /// <summary>
        /// Saves the current read state (header and body stream) for the next read event.
        /// </summary>
        private void StoreReadState(FrameHeader header, MemoryStream stream, byte[] buffer, int offset, int length,
                                    bool hasReadFromStream)
        {
            MemoryStream nextMessageStream;
            if (!hasReadFromStream && stream != null)
            {
                // There hasn't been any operations completed with this buffer, reuse the current stream
                nextMessageStream = stream;
            }
            else
            {
                // Allocate a new stream for store in it
                nextMessageStream = Configuration.BufferPool.GetStream(Connection.StreamReadTag);
            }
            nextMessageStream.Write(buffer, offset, length - offset);
            Volatile.Write(ref _readStream, nextMessageStream);
            Volatile.Write(ref _receivingHeader, header);
            if (_isCanceled)
            {
                // Connection was disposed since we started to store the buffer, try to dispose the stream
                Interlocked.Exchange(ref _readStream, null)?.Dispose();
            }
        }

        /// <summary>
        /// Returns an action that capture the parameters closure
        /// </summary>
        private Action<MemoryStream, long> CreateResponseAction(
            ResultMetadata resultMetadata, ISerializer serializer, FrameHeader header, Action<IRequestError, Response, long> callback)
        {
            var compressor = Compressor;

            void DeserializeResponseStream(MemoryStream stream, long timestamp)
            {
                Response response = null;
                IRequestError error = null;
                var nextPosition = stream.Position + header.BodyLength;
                try
                {
                    Stream plainTextStream = stream;
                    if (header.Flags.HasFlag(HeaderFlags.Compression))
                    {
                        plainTextStream = compressor.Decompress(new WrappedStream(stream, header.BodyLength));
                        plainTextStream.Position = 0;
                    }
                    response = FrameParser.Parse(new Frame(header, plainTextStream, serializer, resultMetadata));
                }
                catch (Exception caughtException)
                {
                    error = RequestError.CreateClientError(caughtException, false);
                }
                if (response is ErrorResponse errorResponse)
                {
                    error = RequestError.CreateServerError(errorResponse);
                    response = null;
                }
                //We must advance the position of the stream manually in case it was not correctly parsed
                stream.Position = nextPosition;
                callback(error, response, timestamp);
            }

            return DeserializeResponseStream;
        }

        /// <summary>
        /// Invokes the callbacks using the default TaskScheduler.
        /// </summary>
        /// <returns>Returns true if one or more callback has been invoked.</returns>
        private static bool InvokeReadCallbacks(MemoryStream stream, ICollection<Action<MemoryStream, long>> operationCallbacks, long timestamp)
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
                    cb(stream, timestamp);
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
            var request = _startupRequestFactory.CreateStartupRequest(Options);
            // Use the Connect timeout for the startup request timeout
            return Send(request, Configuration.SocketOptions.ConnectTimeoutMillis);
        }

        /// <inheritdoc />
        public Task<Response> Send(IRequest request, int timeoutMillis)
        {
            var tcs = new TaskCompletionSource<Response>();
            Send(request, tcs.TrySetRequestError, timeoutMillis);
            return tcs.Task;
        }

        /// <inheritdoc />
        public Task<Response> Send(IRequest request)
        {
            return Send(request, Configuration.DefaultRequestOptions.ReadTimeoutMillis);
        }

        /// <inheritdoc />
        public OperationState Send(IRequest request, Action<IRequestError, Response> callback, int timeoutMillis)
        {
            if (_isCanceled)
            {
                // Avoid calling back before returning
                Task.Factory.StartNew(() => callback(RequestError.CreateClientError(new SocketException((int)SocketError.NotConnected), true), null),
                    CancellationToken.None, TaskCreationOptions.None, TaskScheduler.Default);
                return null;
            }

            IncrementInFlight();

            var state = new OperationState(
                callback,
                request,
                timeoutMillis,
                _connectionObserver.CreateOperationObserver()
            );

            if (state.TimeoutMillis > 0)
            {
                // timer can be disposed while connection cancellation hasn't been invoked yet
                try
                {
                    var requestTimeout = Configuration.Timer.NewTimeout(OnTimeout, state, state.TimeoutMillis);
                    state.SetTimeout(requestTimeout);
                }
                catch (Exception ex)
                {
                    // Avoid calling back before returning
                    Task.Factory.StartNew(() => callback(RequestError.CreateClientError(ex, true), null),
                        CancellationToken.None, TaskCreationOptions.None, TaskScheduler.Default);
                    return null;
                }
            }
            
            _writeQueue.Enqueue(state);
            RunWriteQueue();
            return state;
        }

        /// <inheritdoc />
        public OperationState Send(IRequest request, Action<IRequestError, Response> callback)
        {
            return Send(request, callback, Configuration.DefaultRequestOptions.ReadTimeoutMillis);
        }

        private void RunWriteQueue()
        {
            var previousState = Interlocked.CompareExchange(ref _writeState, Connection.WriteStateRunning, Connection.WriteStateInit);
            if (previousState == Connection.WriteStateRunning)
            {
                // There is another thread writing to the wire
                return;
            }
            if (previousState == Connection.WriteStateClosed)
            {
                // Probably there is an item in the write queue, we should cancel pending
                // Avoid canceling in the user thread
                Task.Factory.StartNew(() => CancelPending(null), CancellationToken.None, TaskCreationOptions.None, TaskScheduler.Default);
                return;
            }
            // Start a new task using the TaskScheduler for writing to avoid using the User thread
            Task.Factory.StartNew(RunWriteQueueAction, CancellationToken.None, TaskCreationOptions.None, TaskScheduler.Default);
        }

        private long GetTimestamp()
        {
            return _timerEnabled ? Stopwatch.GetTimestamp() : 0L;
        }

        private void RunWriteQueueAction()
        {
            //Dequeue all items until threshold is passed
            long totalLength = 0;
            RecyclableMemoryStream stream = null;
            var timestamp = GetTimestamp();
            while (totalLength < Connection.CoalescingThreshold)
            {
                OperationState state = null;
                while (_writeQueue.TryDequeue(out var tempState))
                {
                    if (tempState.CanBeWritten())
                    {
                        state = tempState;
                        break;
                    }

                    DecrementInFlight();
                }

                if (state == null)
                {
                    //No more items in the write queue
                    break;
                }

                if (!_freeOperations.TryPop(out short streamId))
                {
                    //Queue it up for later.
                    _writeQueue.Enqueue(state);
                    //When receiving the next complete message, we can process it.
                    Connection.Logger.Info("Enqueued, no streamIds available. If this message is recurrent consider configuring more connections per host or lower the pressure");
                    break;
                }
                Connection.Logger.Verbose("Sending #{0} for {1} to {2}", streamId, state.Request.GetType().Name, EndPoint.EndpointFriendlyName);
                if (_isCanceled)
                {
                    DecrementInFlight();
                    state.InvokeCallback(RequestError.CreateClientError(new SocketException((int)SocketError.NotConnected), true), timestamp);
                    break;
                }
                _pendingOperations.AddOrUpdate(streamId, state, (k, oldValue) => state);
                var startLength = stream?.Length ?? 0;
                try
                {
                    //lazy initialize the stream
                    stream = stream ?? (RecyclableMemoryStream)Configuration.BufferPool.GetStream(Connection.StreamWriteTag);
                    var frameLength = state.WriteFrame(streamId, stream, Serializer, timestamp);
                    _connectionObserver.OnBytesSent(frameLength);
                    totalLength += frameLength;
                }
                catch (Exception ex)
                {
                    //There was an error while serializing or begin sending
                    Connection.Logger.Error(ex);
                    //The request was not written, clear it from pending operations
                    RemoveFromPending(streamId);
                    //Callback with the Exception
                    state.InvokeCallback(RequestError.CreateClientError(ex, true), timestamp);

                    //Reset the stream to before we started writing this frame
                    stream?.SetLength(startLength);
                    break;
                }
            }
            if (totalLength == 0L)
            {
                // Nothing to write, set the queue as not running
                Interlocked.CompareExchange(ref _writeState, Connection.WriteStateInit, Connection.WriteStateRunning);
                // Until now, we were preventing other threads to running the queue.
                // Check if we can now write:
                // a read could have finished (freeing streamIds) or new request could have been added to the queue
                if (!_freeOperations.IsEmpty && !_writeQueue.IsEmpty)
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
        protected internal virtual OperationState RemoveFromPending(short streamId)
        {
            if (_pendingOperations.TryRemove(streamId, out var state))
            {
                DecrementInFlight();
            }
            //Set the streamId as available
            _freeOperations.Push(streamId);
            return state;
        }

        /// <summary>
        /// Sets the keyspace of the connection.
        /// If the keyspace is different from the current value, it sends a Query request to change it
        /// </summary>
        public async Task<bool> SetKeyspace(string value)
        {
            if (string.IsNullOrEmpty(value))
            {
                return true;
            }
            while (_keyspace != value)
            {
                var switchTcs = Volatile.Read(ref _keyspaceSwitchTcs);
                if (switchTcs != null)
                {
                    // Is already switching
                    await switchTcs.Task.ConfigureAwait(false);
                    continue;
                }

                var tcs = new TaskCompletionSource<bool>();
                switchTcs = Interlocked.CompareExchange(ref _keyspaceSwitchTcs, tcs, null);
                if (switchTcs != null)
                {
                    // Is already switching
                    await switchTcs.Task.ConfigureAwait(false);
                    continue;
                }

                Exception sendException = null;

                // CAS operation won, this is the only thread changing the keyspace
                // but another thread might have changed it in the meantime
                if (_keyspace != value)
                {
                    Connection.Logger.Info("Connection to host {0} switching to keyspace {1}", EndPoint.EndpointFriendlyName, value);
                    var request = new QueryRequest(Serializer, $"USE \"{value}\"", QueryProtocolOptions.Default, false, null);
                    try
                    {
                        await Send(request).ConfigureAwait(false);
                        _keyspace = value;
                    }
                    catch (Exception ex)
                    {
                        sendException = ex;
                    }
                }

                // Set the reference to null before setting the result
                Interlocked.Exchange(ref _keyspaceSwitchTcs, null);
                tcs.TrySet(sendException, true);
                return await tcs.Task.ConfigureAwait(false);
            }
            return true;
        }

        private void OnTimeout(object stateObj)
        {
            var state = (OperationState)stateObj;
            var ex = new OperationTimedOutException(EndPoint, state.TimeoutMillis);
            //Invoke if it hasn't been invoked yet
            //Once the response is obtained, we decrement the timed out counter
            var timedout = state.MarkAsTimedOut(ex, () => Interlocked.Decrement(ref _timedOutOperations), GetTimestamp());
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
            WriteCompleted?.Invoke();

            //There is no need for synchronization here
            //Only 1 thread can be here at the same time.
            //Set the idle timeout to avoid idle disconnects
            if (_heartBeatInterval > 0 && !_isCanceled)
            {
                try
                {
                    _idleTimer.Change(_heartBeatInterval, Timeout.Infinite);
                }
                catch (ObjectDisposedException)
                {
                    //This connection is being disposed
                    //Don't mind
                }
            }
            Interlocked.CompareExchange(ref _writeState, Connection.WriteStateInit, Connection.WriteStateRunning);
            //Send the next request, if exists
            //It will use a new thread
            RunWriteQueue();
        }
    }
}