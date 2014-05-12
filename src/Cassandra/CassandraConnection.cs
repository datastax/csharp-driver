//
//      Copyright (C) 2012 DataStax Inc.
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
using System.Net;
using System.Net.Security;
using System.Net.Sockets;
using System.Security.Cryptography.X509Certificates;
using System.Threading;

namespace Cassandra
{
    internal class CassandraConnection : IDisposable
    {
        private static readonly Logger Logger = new Logger(typeof (CassandraConnection));
        private static readonly FrameParser FrameParser = new FrameParser();
        private readonly BoolSwitch _alreadyDisposed = new BoolSwitch();
        private readonly IAuthInfoProvider _authInfoProvider;
        private readonly IAuthProvider _authProvider;
        private readonly byte[][] _buffer;
        private readonly IBuffering _bufferingMode;
        private readonly IProtoBufComporessor _compressor;

        private readonly AtomicValue<Action<ResponseFrame>> _frameEventCallback = new AtomicValue<Action<ResponseFrame>>(null);
        private readonly AtomicArray<AsyncResult<IOutput>> _frameReadAsyncResult = new AtomicArray<AsyncResult<IOutput>>(sbyte.MaxValue + 1);
        private readonly AtomicArray<Action<ResponseFrame>> _frameReadCallback = new AtomicArray<Action<ResponseFrame>>(sbyte.MaxValue + 1);
        private readonly AtomicArray<Timer> _frameReadTimers = new AtomicArray<Timer>(sbyte.MaxValue + 1);
        private readonly ConcurrentStack<int> _freeStreamIDs = new ConcurrentStack<int>();
        private readonly AtomicValue<bool> _isStreamOpened = new AtomicValue<bool>(false);

        private readonly ISession _owner;
        private readonly int _port;
        private readonly Action<ErrorActionParam> _protocolErrorHandlerAction;
        private readonly int _queryAbortTimeout = Timeout.Infinite;
        private readonly AutoResetEvent _readerSocketStreamBusy = new AutoResetEvent(true);
        private readonly IPAddress _serverAddress;
        private readonly Socket _socket;
        private readonly BoolSwitch _socketExceptionOccured = new BoolSwitch();

        private readonly SocketOptions _socketOptions;
        private readonly Stream _socketStream;

        private readonly Dictionary<string, string> _startupOptions = new Dictionary<string, string>
        {
            {"CQL_VERSION", "3.0.0"}
        };

        private readonly AtomicValue<string> _currentKs = new AtomicValue<string>("");
        private readonly ConcurrentDictionary<byte[], string> _preparedQueries = new ConcurrentDictionary<byte[], string>();
        private readonly AtomicValue<string> _selectedKs = new AtomicValue<string>("");

        internal Guid Guid;
        private volatile byte _binaryProtocolRequestVersionByte = RequestFrame.ProtocolV2RequestVersionByte;
        private volatile byte _binaryProtocolResponseVersionByte = ResponseFrame.ProtocolV2ResponseVersionByte;

        private int _bufNo;
        private Action<ResponseFrame> _defaultFatalErrorAction;

        public bool IsHealthy
        {
            get { return !_alreadyDisposed.IsTaken() && !_socketExceptionOccured.IsTaken(); }
        }

        internal CassandraConnection(ISession owner, IPAddress serverAddress, ProtocolOptions protocolOptions,
                                     SocketOptions socketOptions, ClientOptions clientOptions,
                                     IAuthProvider authProvider, IAuthInfoProvider authInfoProvider, int protocolVersion)
        {
            if (protocolVersion == 1)
            {
                _binaryProtocolRequestVersionByte = RequestFrame.ProtocolV1RequestVersionByte;
                _binaryProtocolResponseVersionByte = ResponseFrame.ProtocolV1ResponseVersionByte;
            }

            Guid = Guid.NewGuid();
            _owner = owner;
            _bufferingMode = null;
            switch (protocolOptions.Compression)
            {
                case CompressionType.LZ4:
                    _bufferingMode = new FrameBuffering();
                    break;
                case CompressionType.Snappy:
                    _bufferingMode = new FrameBuffering();
                    break;
                case CompressionType.NoCompression:
                    _bufferingMode = clientOptions.WithoutRowSetBuffering ? new NoBuffering() : new FrameBuffering();
                    break;
                default:
                    throw new ArgumentException();
            }

            _authProvider = authProvider;
            _authInfoProvider = authInfoProvider;
            if (protocolOptions.Compression == CompressionType.Snappy)
            {
                _startupOptions.Add("COMPRESSION", "snappy");
                _compressor = new SnappyProtoBufCompressor();
            }
            else if (protocolOptions.Compression == CompressionType.LZ4)
            {
                _startupOptions.Add("COMPRESSION", "lz4");
                _compressor = new LZ4ProtoBufCompressor();
            }

            _serverAddress = serverAddress;
            _port = protocolOptions.Port;
            _queryAbortTimeout = clientOptions.QueryAbortTimeout;

            _socketOptions = socketOptions;

            for (int i = 0; i <= sbyte.MaxValue; i++)
                _freeStreamIDs.Push(i);

            _protocolErrorHandlerAction = param =>
            {
                if (param.AbstractResponse is ErrorResponse)
                    JobFinished(
                        param.Jar,
                        (param.AbstractResponse as ErrorResponse).Output);
            };

            _frameEventCallback.Value = EventOccured;

            _buffer = new[]
            {
                new byte[_bufferingMode.PreferedBufferSize()],
                new byte[_bufferingMode.PreferedBufferSize()]
            };

            var newSock = new Socket(AddressFamily.InterNetwork, SocketType.Stream, ProtocolType.Tcp);

            if (_socketOptions.KeepAlive != null)
                newSock.SetSocketOption(SocketOptionLevel.Socket, SocketOptionName.KeepAlive,
                                        _socketOptions.KeepAlive.Value);

            newSock.SendTimeout = _socketOptions.ConnectTimeoutMillis;

            if (_socketOptions.SoLinger != null)
                newSock.SetSocketOption(SocketOptionLevel.Socket, SocketOptionName.Linger,
                                        new LingerOption(true, _socketOptions.SoLinger.Value));

            if (_socketOptions.ReceiveBufferSize != null)
                newSock.ReceiveBufferSize = _socketOptions.ReceiveBufferSize.Value;

            if (_socketOptions.SendBufferSize != null)
                newSock.ReceiveBufferSize = _socketOptions.SendBufferSize.Value;

            if (_socketOptions.TcpNoDelay != null)
                newSock.NoDelay = _socketOptions.TcpNoDelay.Value;


            var connectionResult = newSock.BeginConnect(_serverAddress, _port, null, null);
            connectionResult.AsyncWaitHandle.WaitOne(_socketOptions.ConnectTimeoutMillis);

            if (!newSock.Connected)
            {
                //The socket is still open
                newSock.Close();
                throw new SocketException((int)SocketError.TimedOut);
            }

            _socket = newSock;
            _bufferingMode.Reset();

            if (protocolOptions.SslOptions == null)
                _socketStream = new NetworkStream(_socket);
            else
            {
                string targetHost;
                try
                {
                    targetHost = Dns.GetHostEntry(_serverAddress).HostName;
                }
                catch (SocketException ex)
                {
                    targetHost = serverAddress.ToString();
                    Logger.Error(
                        String.Format(
                            "SSL connection: Can not resolve address {0}. Using the IP address instead of the hostname. This may cause RemoteCertificateNameMismatch error during Cassandra host authentication. Note that the Cassandra node SSL certificate's CN(Common Name) must match the Cassandra node hostname.",
                            _serverAddress), ex);
                }

                _socketStream = new SslStream(new NetworkStream(_socket), false, protocolOptions.SslOptions.RemoteCertValidationCallback, null);
                (_socketStream as SslStream).AuthenticateAsClient(targetHost, new X509CertificateCollection(), protocolOptions.SslOptions.SslProtocol,
                                                                  false);
            }

            if (IsHealthy)
                BeginReading();
        }

        public void Dispose()
        {
            if (!_alreadyDisposed.TryTake())
                return;

            try
            {
                if (_socket != null)
                {
                    _socket.Shutdown(SocketShutdown.Both);
                    _socket.Disconnect(_socketOptions.ReuseAddress ?? false);
                    _socket.Close();
                }
            }
            catch (ObjectDisposedException)
            {
            }
            catch (SocketException)
            {
            }
        }

        protected virtual void HostIsDown()
        {
            if (_owner is Session)
            {
                ((Session)_owner).HostIsDown(_serverAddress);
            }
        }

        internal int AllocateStreamId()
        {
            int ret;
            if (_freeStreamIDs.TryPop(out ret))
                return ret;
            throw new StreamAllocationException();
        }

        internal void FreeStreamId(int streamId)
        {
            _freeStreamIDs.Push(streamId);
        }

        public bool IsBusy(int max)
        {
            return sbyte.MaxValue + 1 - _freeStreamIDs.Count >= max;
        }

        public bool IsFree(int min)
        {
            return sbyte.MaxValue + 1 - _freeStreamIDs.Count <= min;
        }

        public bool IsEmpty()
        {
            return _freeStreamIDs.Count == sbyte.MaxValue + 1;
        }

        private void JobFinished(AsyncResult<IOutput> jar, IOutput outp)
        {
            try
            {
                FreeStreamId(jar.StreamId);
            }
            finally
            {
                jar.SetResult(outp);
                jar.Complete();
                (outp as IWaitableForDispose).WaitForDispose();
            }
        }

        private AsyncResult<IOutput> SetupJob(int streamId, AsyncCallback callback, object state, object owner, string propId)
        {
            var ar = new AsyncResult<IOutput>(streamId, callback, state, owner, propId, null, null);

            _defaultFatalErrorAction = frame2 =>
            {
                AbstractResponse response2 = FrameParser.Parse(frame2);
                _protocolErrorHandlerAction(new ErrorActionParam {AbstractResponse = response2, Jar = ar});
            };


            _frameReadAsyncResult[streamId] = ar;

            return ar;
        }

        private void BeginJob(AsyncResult<IOutput> jar, Action job, bool startup = true)
        {
            try
            {
                if (startup && !_isStreamOpened.Value)
                {
                    Evaluate(new StartupRequest(jar.StreamId, _startupOptions), jar.StreamId, frame =>
                    {
                        AbstractResponse response = FrameParser.Parse(frame);
                        if (response is ReadyResponse)
                        {
                            _isStreamOpened.Value = true;
                            job();
                        }
                        else if (response is AuthenticateResponse)
                        {
                            if (_binaryProtocolRequestVersionByte == RequestFrame.ProtocolV1RequestVersionByte &&
                                _authProvider == NoneAuthProvider.Instance)
                                //this should be true only if we have v1 protocol and it is not DSE 
                            {
                                IDictionary<string, string> credentials = _authInfoProvider.GetAuthInfos(_serverAddress);

                                Evaluate(new CredentialsRequest(jar.StreamId, credentials), jar.StreamId, frame2 =>
                                {
                                    AbstractResponse response2 = FrameParser.Parse(frame2);
                                    if (response2 is ReadyResponse)
                                    {
                                        _isStreamOpened.Value = true;
                                        job();
                                    }
                                    else
                                        _protocolErrorHandlerAction(new ErrorActionParam {AbstractResponse = response2, Jar = jar});
                                });
                            }
                            else
                                //either DSE or protocol V2 (or both)
                            {
                                IAuthenticator authenticator = _authProvider.NewAuthenticator(_serverAddress);

                                byte[] initialResponse = authenticator.InitialResponse();
                                if (null == initialResponse)
                                    initialResponse = new byte[0];

                                WaitForSaslResponse(jar, initialResponse, authenticator, job);
                            }
                        }
                        else
                            _protocolErrorHandlerAction(new ErrorActionParam {AbstractResponse = response, Jar = jar});
                    });
                }
                else
                    job();
            }
            catch (QueryValidationException)
            {
                throw;
            }
            catch (Exception ex)
            {
                if (!SetupSocketException(ex))
                    throw;
            }
        }

        private void WaitForSaslResponse(AsyncResult<IOutput> jar, byte[] response, IAuthenticator authenticator, Action job)
        {
            Evaluate(new AuthResponseRequest(jar.StreamId, response), jar.StreamId, frame2 =>
            {
                AbstractResponse response2 = FrameParser.Parse(frame2);
                if ((response2 is AuthSuccessResponse)
                    || (response2 is ReadyResponse))
                {
                    _isStreamOpened.Value = true;
                    job();
                }
                else if (response2 is AuthChallengeResponse)
                {
                    byte[] responseToServer = authenticator.EvaluateChallenge((response2 as AuthChallengeResponse).Token);
                    if (responseToServer == null)
                    {
                        // If we generate a null response, then authentication has completed,return without
                        // sending a further response back to the server.
                        _isStreamOpened.Value = true;
                        job();
                    }
                    // Otherwise, send the challenge response back to the server
                    WaitForSaslResponse(jar, responseToServer, authenticator, job);
                }
                else
                    _protocolErrorHandlerAction(new ErrorActionParam {AbstractResponse = response2, Jar = jar});
            });
        }

        private void AbortTimerProc(object state)
        {
            var streamId = (int) state;

            SetupSocketException(new CassandraConnectionTimeoutException());

            try
            {
                _socketStream.Close();
                _socket.Shutdown(SocketShutdown.Both);
                _socket.Disconnect(_socketOptions.ReuseAddress ?? false);
                _socket.Close();
            }
            catch (Exception ex)
            {
                if (!IsStreamRelatedException(ex))
                    throw;
            }

            try
            {
                _bufferingMode.Close();
            }
            catch (Exception ex)
            {
                if (!IsStreamRelatedException(ex))
                    throw;
            }
        }

        private void BeginReading()
        {
            try
            {
                if (!(_bufferingMode is FrameBuffering))
                    _readerSocketStreamBusy.WaitOne();

                IAsyncResult rh = _socketStream.BeginRead(_buffer[_bufNo], 0, _buffer[_bufNo].Length, ar =>
                {
                    try
                    {
                        // when already disposed, _socketStream.EndRead() throws exception
                        // when accessing disposed instance _socket internally.
                        // so need to check disposed first
                        if (_alreadyDisposed.IsTaken())
                        {
                            ForceComplete();
                            return;
                        }

                        int bytesReadCount = _socketStream.EndRead(ar);
                        if (bytesReadCount == 0)
                        {
                            if (_alreadyDisposed.IsTaken())
                            {
                                ForceComplete();
                                return;
                            }

                            throw new CassandraConnectionIOException();
                        }
                        else
                        {
                            foreach (ResponseFrame frame in _bufferingMode.Process(_buffer[_bufNo], bytesReadCount, _socketStream, _compressor))
                            {
                                if (frame.FrameHeader.Version != _binaryProtocolResponseVersionByte)
                                    throw new CassandraConnectionBadProtocolVersionException();

                                Action<ResponseFrame> act = null;
                                if (frame.FrameHeader.StreamId == 0xFF)
                                    act = _frameEventCallback.Value;
                                else if (frame.FrameHeader.StreamId <= sbyte.MaxValue)
                                {
                                    if (_frameReadTimers[frame.FrameHeader.StreamId] != null)
                                        _frameReadTimers[frame.FrameHeader.StreamId].Change(Timeout.Infinite,
                                                                                            Timeout.Infinite);
                                    act = _frameReadCallback[frame.FrameHeader.StreamId];
                                    _frameReadCallback[frame.FrameHeader.StreamId] = null;
                                }

                                if (act == null)
                                {
                                    throw new InvalidOperationException("Protocol error! Unmatched response. Terminating all requests now...");
                                }

                                act.BeginInvoke(frame, tar =>
                                {
                                    try
                                    {
                                        (tar.AsyncState as Action<ResponseFrame>).EndInvoke(tar);
                                    }
                                    catch (Exception ex)
                                    {
                                        SetupSocketException(ex);
                                    }
                                    finally
                                    {
                                        if (!(_bufferingMode is FrameBuffering))
                                            if (IsHealthy)
                                                BeginReading();
                                    }
                                }, act);
                            }
                            _bufNo = 1 - _bufNo;
                        }
                    }
                    catch (Exception ex)
                    {
                        SetupSocketException(ex);
                    }
                    finally
                    {
                        if (_bufferingMode is FrameBuffering)
                        {
                            if (IsHealthy)
                                BeginReading();
                        }
                        else
                            _readerSocketStreamBusy.Set();
                    }
                }, null);
            }
            catch (IOException e)
            {
                if (!SetupSocketException(e))
                    throw;
            }
        }

        internal static bool IsStreamRelatedException(Exception ex)
        {
            return ex is SocketException
                   || ex is CassandraConnectionIOException
                   || ex is IOException
                   || ex is ObjectDisposedException
                   || ex is StreamAllocationException
                   || ex is CassandraConnectionTimeoutException;
        }

        private void ForceComplete(Exception ex = null)
        {
            for (int streamId = 0; streamId < sbyte.MaxValue + 1; streamId++)
            {
                if (_frameReadTimers[streamId] != null)
                    _frameReadTimers[streamId].Change(Timeout.Infinite, Timeout.Infinite);
                AsyncResult<IOutput> ar = _frameReadAsyncResult[streamId];
                if (ar != null && !ar.IsCompleted)
                    _frameReadAsyncResult[streamId].Complete(ex ?? new CassandraConnectionIOException());
            }
        }

        private bool SetupSocketException(Exception ex)
        {
            ForceComplete(ex);

            HostIsDown();
            try
            {
                _bufferingMode.Close();
            }
            catch
            {
            }

            _socketExceptionOccured.TryTake();
            return (ex.InnerException != null && IsStreamRelatedException(ex.InnerException)) || IsStreamRelatedException(ex);
        }

        private void CheckDisposed()
        {
            if (_alreadyDisposed.IsTaken())
                throw new ObjectDisposedException("CassandraConnection");
        }

        ~CassandraConnection()
        {
            Dispose();
        }

        private void Evaluate(IRequest req, int streamId, Action<ResponseFrame> nextAction)
        {
            try
            {
                RequestFrame frame = req.GetFrame(_binaryProtocolRequestVersionByte);
                lock (_socketStream)
                {
                    _frameReadCallback[streamId] = nextAction;
                    if (_queryAbortTimeout != Timeout.Infinite)
                    {
                        if (_frameReadTimers[streamId] == null)
                            _frameReadTimers[streamId] = new Timer(AbortTimerProc, streamId, _queryAbortTimeout, Timeout.Infinite);
                        else
                            _frameReadTimers[streamId].Change(_queryAbortTimeout, Timeout.Infinite);
                    }
                    frame.Buffer.WriteTo(_socketStream);
                }
            }
            catch (InvalidQueryException)
            {
                //The socket is OK, it is just an exception
                throw;
            }
            catch (Exception ex)
            {
                if (!SetupSocketException(ex))
                {
                    throw;
                }
            }
        }

        internal IPAddress GetHostAdress()
        {
            return _serverAddress;
        }

        public event CassandraEventHandler CassandraEvent;

        private void EventOccured(ResponseFrame frame)
        {
            AbstractResponse response = FrameParser.Parse(frame);
            if (response is EventResponse)
            {
                if (CassandraEvent != null)
                    CassandraEvent.Invoke(this, (response as EventResponse).CassandraEventArgs);
                return;
            }
            throw new DriverInternalError("Unexpected response frame");
        }

        public IAsyncResult BeginRegisterForCassandraEvent(int streamId, CassandraEventType eventTypes, AsyncCallback callback, object state,
                                                           object owner)
        {
            AsyncResult<IOutput> jar = SetupJob(streamId, callback, state, owner, "REGISTER");
            BeginJob(jar, () =>
            {
                Evaluate(new RegisterForEventRequest(jar.StreamId, eventTypes), jar.StreamId, frame2 =>
                {
                    AbstractResponse response = FrameParser.Parse(frame2);
                    if (response is ReadyResponse)
                        JobFinished(jar, new OutputVoid(null));
                    else
                        _protocolErrorHandlerAction(new ErrorActionParam {AbstractResponse = response, Jar = jar});
                });
            });
            return jar;
        }

        public IOutput EndRegisterForCassandraEvent(IAsyncResult result, object owner)
        {
            return AsyncResult<IOutput>.End(result, owner, "REGISTER");
        }

        public IOutput RegisterForCassandraEvent(int streamId, CassandraEventType eventTypes)
        {
            return EndRegisterForCassandraEvent(BeginRegisterForCassandraEvent(streamId, eventTypes, null, null, this), this);
        }

        private Dictionary<byte[], string> NotContainsInAlreadyPrepared(Dictionary<byte[], string> ids)
        {
            var ret = new Dictionary<byte[], string>();
            foreach (byte[] key in ids.Keys)
            {
                if (!_preparedQueries.ContainsKey(key))
                    ret.Add(key, ids[key]);
            }
            return ret;
        }

        public Action SetupPreparedQueries(AsyncResult<IOutput> jar, Dictionary<byte[], string> ids, Action dx)
        {
            return () =>
            {
                Dictionary<byte[], string> ncip = NotContainsInAlreadyPrepared(ids);
                if (ncip.Count > 0)
                {
                    foreach (KeyValuePair<byte[], string> ncipit in ncip)
                    {
                        Evaluate(new PrepareRequest(jar.StreamId, ncipit.Value), jar.StreamId, frame2 =>
                        {
                            AbstractResponse response = FrameParser.Parse(frame2);
                            if (response is ResultResponse)
                            {
                                _preparedQueries.TryAdd(ncipit.Key, ncipit.Value);
                                BeginJob(jar, SetupPreparedQueries(jar, ids, dx));
                            }
                            else
                                _protocolErrorHandlerAction(new ErrorActionParam {AbstractResponse = response, Jar = jar});
                        });
                        break;
                    }
                }
                else
                    dx();
            };
        }

        private Dictionary<byte[], string> GetIdsFromListOfQueries(List<Statement> queries)
        {
            var ret = new Dictionary<byte[], string>();
            foreach (Statement q in queries)
            {
                if (q is BoundStatement)
                {
                    var bs = (q as BoundStatement);
                    if (!ret.ContainsKey(bs.PreparedStatement.Id))
                        ret.Add(bs.PreparedStatement.Id, bs.PreparedStatement.Cql);
                }
            }
            return ret;
        }

        private List<IQueryRequest> GetRequestsFromListOfQueries(List<Statement> queries)
        {
            var ret = new List<IQueryRequest>();
            foreach (Statement q in queries)
                ret.Add(q.CreateBatchRequest());
            return ret;
        }

        public IAsyncResult BeginBatch(int streamId, BatchType batchType, List<Statement> queries,
                                       AsyncCallback callback, object state, object owner,
                                       ConsistencyLevel consistency, bool isTracing)
        {
            AsyncResult<IOutput> jar = SetupJob(streamId, callback, state, owner, "BATCH");


            BeginJob(jar, SetupKeyspace(jar, SetupPreparedQueries(jar, GetIdsFromListOfQueries(queries), () =>
            {
                Evaluate(new BatchRequest(jar.StreamId, batchType, GetRequestsFromListOfQueries(queries), consistency, isTracing), jar.StreamId,
                         frame2 =>
                         {
                             AbstractResponse response = FrameParser.Parse(frame2);
                             if (response is ResultResponse)
                                 JobFinished(jar, (response as ResultResponse).Output);
                             else
                                 _protocolErrorHandlerAction(new ErrorActionParam
                                 {
                                     AbstractResponse = response,
                                     Jar = jar
                                 });
                         });
            })));

            return jar;
        }

        public IOutput EndBatch(IAsyncResult result, object owner)
        {
            return AsyncResult<IOutput>.End(result, owner, "BATCH");
        }

        public Action SetupPreparedQuery(AsyncResult<IOutput> jar, byte[] id, string cql, Action dx)
        {
            return () =>
            {
                if (!_preparedQueries.ContainsKey(id))
                {
                    Evaluate(new PrepareRequest(jar.StreamId, cql), jar.StreamId, frame2 =>
                    {
                        AbstractResponse response = FrameParser.Parse(frame2);
                        if (response is ResultResponse)
                        {
                            _preparedQueries.TryAdd(id, cql);
                            dx();
                        }
                        else
                            _protocolErrorHandlerAction(new ErrorActionParam {AbstractResponse = response, Jar = jar});
                    });
                }
                else
                    dx();
            };
        }

        public IAsyncResult BeginExecuteQuery(int streamId, byte[] id, string cql, RowSetMetadata metadata,
                                              AsyncCallback callback, object state, object owner,
                                              bool isTracing, QueryProtocolOptions queryProtocolOptions, ConsistencyLevel? consistency = null)
        {
            AsyncResult<IOutput> jar = SetupJob(streamId, callback, state, owner, "EXECUTE");

            BeginJob(jar, SetupKeyspace(jar, SetupPreparedQuery(jar, id, cql, () =>
            {
                Evaluate(new ExecuteRequest(jar.StreamId, id, metadata, isTracing, queryProtocolOptions, consistency), jar.StreamId,
                         frame2 =>
                         {
                             AbstractResponse response = FrameParser.Parse(frame2);
                             if (response is ResultResponse)
                                 JobFinished(jar, (response as ResultResponse).Output);
                             else
                                 _protocolErrorHandlerAction(new ErrorActionParam
                                 {
                                     AbstractResponse = response,
                                     Jar = jar
                                 });
                         });
            })));

            return jar;
        }

        public IOutput EndExecuteQuery(IAsyncResult result, object owner)
        {
            return AsyncResult<IOutput>.End(result, owner, "EXECUTE");
        }

        public IOutput ExecuteQuery(int streamId, byte[] id, string cql, RowSetMetadata metadata,
                                    bool isTracing, QueryProtocolOptions queryPrtclOptions, ConsistencyLevel? consistency)
        {
            return EndExecuteQuery(BeginExecuteQuery(streamId, id, cql, metadata, null, null, this, isTracing, queryPrtclOptions, consistency),
                                   this);
        }

        public IAsyncResult BeginExecuteQueryCredentials(int streamId, IDictionary<string, string> credentials, AsyncCallback callback, object state,
                                                         object owner)
        {
            AsyncResult<IOutput> jar = SetupJob(streamId, callback, state, owner, "CREDENTIALS");
            BeginJob(jar, () =>
            {
                Evaluate(new CredentialsRequest(jar.StreamId, credentials), jar.StreamId, frame2 =>
                {
                    AbstractResponse response = FrameParser.Parse(frame2);
                    if (response is ReadyResponse)
                        JobFinished(jar, new OutputVoid(null));
                    else
                        _protocolErrorHandlerAction(new ErrorActionParam {AbstractResponse = response, Jar = jar});
                });
            });
            return jar;
        }

        public IOutput EndExecuteQueryCredentials(IAsyncResult result, object owner)
        {
            return AsyncResult<IOutput>.End(result, owner, "CREDENTIALS");
        }

        public IOutput ExecuteCredentials(int streamId, IDictionary<string, string> credentials)
        {
            return EndExecuteQueryCredentials(BeginExecuteQueryCredentials(streamId, credentials, null, null, this), this);
        }

        public void SetKeyspace(string ks)
        {
            _selectedKs.Value = ks;
        }

        public Action SetupKeyspace(AsyncResult<IOutput> jar, Action dx)
        {
            return () =>
            {
                if (!_currentKs.Value.Equals(_selectedKs.Value))
                {
                    Evaluate(new QueryRequest(jar.StreamId, CqlQueryTools.GetUseKeyspaceCql(_selectedKs.Value), false, QueryProtocolOptions.Default),
                             jar.StreamId, frame3 =>
                             {
                                 AbstractResponse response = FrameParser.Parse(frame3);
                                 if (response is ResultResponse)
                                 {
                                     _currentKs.Value = _selectedKs.Value;
                                     dx();
                                 }
                                 else
                                     _protocolErrorHandlerAction(new ErrorActionParam {AbstractResponse = response, Jar = jar});
                             });
                }
                else
                    dx();
            };
        }

        public IAsyncResult BeginQuery(int streamId, string cqlQuery, AsyncCallback callback, object state, object owner, bool tracingEnabled,
                                       QueryProtocolOptions queryPrtclOptions, ConsistencyLevel? consistency = null)
        {
            AsyncResult<IOutput> jar = SetupJob(streamId, callback, state, owner, "QUERY");
            BeginJob(jar, SetupKeyspace(jar, () =>
            {
                Evaluate(new QueryRequest(jar.StreamId, cqlQuery, tracingEnabled, queryPrtclOptions, consistency), jar.StreamId, frame2 =>
                {
                    AbstractResponse response = FrameParser.Parse(frame2);
                    if (response is ResultResponse)
                        JobFinished(jar, (response as ResultResponse).Output);
                    else
                        _protocolErrorHandlerAction(new ErrorActionParam {AbstractResponse = response, Jar = jar});
                });
            }));
            return jar;
        }

        public IOutput EndQuery(IAsyncResult result, object owner)
        {
            return AsyncResult<IOutput>.End(result, owner, "QUERY");
        }

        public IOutput Query(int streamId, string cqlQuery, bool tracingEnabled, QueryProtocolOptions queryPrtclOptions,
                             ConsistencyLevel? consistency = null)
        {
            return EndQuery(BeginQuery(streamId, cqlQuery, null, null, this, tracingEnabled, queryPrtclOptions, consistency), this);
        }

        public IAsyncResult BeginPrepareQuery(int stramId, string cqlQuery, AsyncCallback callback, object state, object owner)
        {
            AsyncResult<IOutput> jar = SetupJob(stramId, callback, state, owner, "PREPARE");
            BeginJob(jar, SetupKeyspace(jar, () =>
            {
                Evaluate(new PrepareRequest(jar.StreamId, cqlQuery), jar.StreamId, frame2 =>
                {
                    AbstractResponse response = FrameParser.Parse(frame2);
                    if (response is ResultResponse)
                    {
                        IOutput outp = (response as ResultResponse).Output;
                        if (outp is OutputPrepared)
                            _preparedQueries[(outp as OutputPrepared).QueryId] = cqlQuery;
                        JobFinished(jar, outp);
                    }
                    else
                        _protocolErrorHandlerAction(new ErrorActionParam {AbstractResponse = response, Jar = jar});
                });
            }));
            return jar;
        }

        public IOutput EndPrepareQuery(IAsyncResult result, object owner)
        {
            return AsyncResult<IOutput>.End(result, owner, "PREPARE");
        }

        public IOutput PrepareQuery(int streamId, string cqlQuery)
        {
            return EndPrepareQuery(BeginPrepareQuery(streamId, cqlQuery, null, null, this), this);
        }

        public IAsyncResult BeginExecuteQueryOptions(int streamId, AsyncCallback callback, object state, object owner)
        {
            AsyncResult<IOutput> jar = SetupJob(streamId, callback, state, owner, "OPTIONS");

            BeginJob(jar, () =>
            {
                Evaluate(new OptionsRequest(jar.StreamId), jar.StreamId, frame2 =>
                {
                    AbstractResponse response = FrameParser.Parse(frame2);
                    if (response is SupportedResponse)
                        JobFinished(jar, (response as SupportedResponse).Output);
                    else
                        _protocolErrorHandlerAction(new ErrorActionParam {AbstractResponse = response, Jar = jar});
                });
            }, true);

            return jar;
        }

        public IOutput EndExecuteQueryOptions(IAsyncResult result, object owner)
        {
            return AsyncResult<IOutput>.End(result, owner, "OPTIONS");
        }

        public IOutput ExecuteOptions(int streamId)
        {
            return EndExecuteQueryOptions(BeginExecuteQueryOptions(streamId, null, null, this), this);
        }

        /// <summary>
        /// Shutdowns the underlying socket for read and writing: Testing purposes
        /// </summary>
        internal void KillSocket()
        {
            try
            {
                _socket.Shutdown(SocketShutdown.Both);
            }
            catch 
            {

            }
        }

        private struct ErrorActionParam
        {
            public AbstractResponse AbstractResponse;
            public AsyncResult<IOutput> Jar;
        }

        internal class StreamAllocationException : Exception
        {
        }
    }
}