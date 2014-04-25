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
﻿using System;
using System.Collections.Generic;
using System.Net;
using System.Net.Sockets;
using System.IO;
using System.Threading;
using System.Collections.Concurrent;
using System.Net.Security;
using System.Security.Cryptography.X509Certificates;

namespace Cassandra
{
    internal enum BufferingMode { NoBuffering, FrameBuffering }

    internal partial class CassandraConnection
    {
#if ERRORINJECTION
        public void KillSocket()
        {
            try
            {
                _socket.Shutdown(SocketShutdown.Both);
            }
            catch { }
        }
#endif
        private static Logger _logger = new Logger(typeof(CassandraConnection));
        readonly IPAddress _serverAddress;
        readonly int _port;
        readonly Socket _socket;
        readonly ConcurrentStack<int> _freeStreamIDs = new ConcurrentStack<int>();
        readonly AtomicValue<bool> _isStreamOpened = new AtomicValue<bool>(false);
        readonly int _queryAbortTimeout = Timeout.Infinite;

        readonly AtomicValue<Action<ResponseFrame>> _frameEventCallback = new AtomicValue<Action<ResponseFrame>>(null);
        readonly AtomicArray<Action<ResponseFrame>> _frameReadCallback = new AtomicArray<Action<ResponseFrame>>(sbyte.MaxValue + 1);
        readonly AtomicArray<AsyncResult<IOutput>> _frameReadAsyncResult = new AtomicArray<AsyncResult<IOutput>>(sbyte.MaxValue + 1);
        readonly AtomicArray<Timer> _frameReadTimers = new AtomicArray<Timer>(sbyte.MaxValue + 1);

        Action<ResponseFrame> _defaultFatalErrorAction;

        struct ErrorActionParam
        {
            public AbstractResponse AbstractResponse;
            public AsyncResult<IOutput> Jar;
        }

        Action<ErrorActionParam> _protocolErrorHandlerAction;

        readonly IAuthProvider _authProvider;
        readonly IAuthInfoProvider _authInfoProvider;

        readonly Session _owner;

        private readonly SocketOptions _socketOptions;        

        void HostIsDown()
        {
            _owner.HostIsDown(_serverAddress);
        }

        internal Guid Guid;

        internal CassandraConnection(Session owner, IPAddress serverAddress, ProtocolOptions protocolOptions,
                                     SocketOptions socketOptions, ClientOptions clientOptions,
                                     IAuthProvider authProvider,
                                     IAuthInfoProvider authInfoProvider)
        {
            this.Guid = Guid.NewGuid();
            this._owner = owner;
            _bufferingMode = null;           
            switch (protocolOptions.Compression)
            {
                case CompressionType.Snappy:
                    _bufferingMode = new FrameBuffering();
                    break;
                case CompressionType.NoCompression:
                    _bufferingMode = clientOptions.WithoutRowSetBuffering ? new NoBuffering() : new FrameBuffering();
                    break;
                default:
                    throw new ArgumentException();
            }

            this._authProvider = authProvider;
            this._authInfoProvider = authInfoProvider;
            if (protocolOptions.Compression == CompressionType.Snappy)
            {
                _startupOptions.Add("COMPRESSION", "snappy");
                _compressor = new SnappyProtoBufCompressor();
            }
            this._serverAddress = serverAddress;
            this._port = protocolOptions.Port;
            this._queryAbortTimeout = clientOptions.QueryAbortTimeout;

            this._socketOptions = socketOptions;

            for (int i = 0; i <= sbyte.MaxValue; i++)
                _freeStreamIDs.Push(i);

            _protocolErrorHandlerAction = new Action<ErrorActionParam>((param) =>
               {
                   if (param.AbstractResponse is ErrorResponse)
                       JobFinished(
                           param.Jar,
                           (param.AbstractResponse as ErrorResponse).Output);
               });

            _frameEventCallback.Value = new Action<ResponseFrame>(EventOccured);

            _buffer = new byte[][] { 
                    new byte[_bufferingMode.PreferedBufferSize()], 
                    new byte[_bufferingMode.PreferedBufferSize()] };

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

            //Avoid waiting more time that expected
            var connectionResult = newSock.BeginConnect(_serverAddress, _port, null, null);
            connectionResult.AsyncWaitHandle.WaitOne(_socketOptions.ConnectTimeoutMillis);

            if (!newSock.Connected)
            {
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
                    _logger.Error(string.Format("SSL connection: Can not resolve {0} address. Using IP address instead of hostname. This may cause RemoteCertificateNameMismatch error during Cassandra host authentication. Note that Cassandra node SSL certificate's CN(Common Name) must match the Cassandra node hostname.", _serverAddress.ToString()), ex);
                }

                _socketStream = new SslStream(new NetworkStream(_socket), false, new RemoteCertificateValidationCallback(protocolOptions.SslOptions.RemoteCertValidationCallback), null);
                (_socketStream as SslStream).AuthenticateAsClient(targetHost, new X509CertificateCollection(), protocolOptions.SslOptions.SslProtocol, false);
            }

            if (IsHealthy)
                BeginReading();
        }

        byte[][] _buffer = null;
        int _bufNo = 0;

        private readonly Stream _socketStream;

        readonly IBuffering _bufferingMode;

        internal int AllocateStreamId()
        {
            int ret;
            if (_freeStreamIDs.TryPop(out ret))
                return ret;
            else
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

        readonly Dictionary<string, string> _startupOptions = new Dictionary<string, string>()
        {
            {"CQL_VERSION","3.0.0"}
        };

        static readonly FrameParser FrameParser = new FrameParser();

        internal class StreamAllocationException : Exception
        {
        }

        private AsyncResult<IOutput> SetupJob(int streamId, AsyncCallback callback, object state, object owner, string propId)
        {
            var ar = new AsyncResult<IOutput>(streamId, callback, state, owner, propId, null, null);

            _defaultFatalErrorAction = new Action<ResponseFrame>((frame2) =>
            {
                var response2 = FrameParser.Parse(frame2);
                _protocolErrorHandlerAction(new ErrorActionParam() { AbstractResponse = response2, Jar = ar });
            });


            _frameReadAsyncResult[streamId] = ar;

            return ar;
        }

        private void BeginJob(AsyncResult<IOutput> jar,  Action job, bool startup = true)
        {

            try
            {
                if (startup && !_isStreamOpened.Value)
                {
                    Evaluate(new StartupRequest(jar.StreamId, _startupOptions), jar.StreamId, (frame) =>
                    {
                        var response = FrameParser.Parse(frame);
                        if (response is ReadyResponse)
                        {
                            _isStreamOpened.Value = true;
                            job();
                        }
                        else if (response is AuthenticateResponse)
                        {
                            if (_authProvider == NoneAuthProvider.Instance) //Apache C*
                            {

                                var credentials = _authInfoProvider.GetAuthInfos(_serverAddress);

                                Evaluate(new CredentialsRequest(jar.StreamId, credentials), jar.StreamId, new Action<ResponseFrame>((frame2) =>
                                {
                                    var response2 = FrameParser.Parse(frame2);
                                    if (response2 is ReadyResponse)
                                    {
                                        _isStreamOpened.Value = true;
                                        job();
                                    }
                                    else
                                        _protocolErrorHandlerAction(new ErrorActionParam() { AbstractResponse = response2, Jar = jar });
                                }));
                            }
                            else //DSE
                            {
                                var authenticator = _authProvider.NewAuthenticator(this._serverAddress);

                                var initialResponse = authenticator.InitialResponse();
                                if (null == initialResponse)
                                    initialResponse = new byte[0];

                                WaitForSaslResponse(jar, initialResponse, authenticator, job);
                            }
                        }
                        else
                            _protocolErrorHandlerAction(new ErrorActionParam() { AbstractResponse = response, Jar = jar});
                    });
                }
                else
                    job();
            }
            catch (Exception ex)
            {
                if (!SetupSocketException(ex))
                    throw;
            }
        }

        private void WaitForSaslResponse(AsyncResult<IOutput> jar, byte[] response, IAuthenticator authenticator, Action job)
        {
            Evaluate(new AuthResponseRequest(jar.StreamId, response), jar.StreamId, new Action<ResponseFrame>((frame2) =>
            {
                var response2 = FrameParser.Parse(frame2);
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
                        return;
                    }
                    else
                    {
                        // Otherwise, send the challenge response back to the server
                        WaitForSaslResponse(jar, responseToServer, authenticator, job);
                    }
                }
                else
                    _protocolErrorHandlerAction(new ErrorActionParam() { AbstractResponse = response2, Jar = jar });
            }));
        }

        readonly IProtoBufComporessor _compressor = null;

        private BoolSwitch _socketExceptionOccured = new BoolSwitch();

        public bool IsHealthy
        {
            get
            {
                return !_alreadyDisposed.IsTaken() && !_socketExceptionOccured.IsTaken();
            }
        }

        private void AbortTimerProc(object state)
        {
            int streamId = (int)state;

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

        readonly AutoResetEvent _readerSocketStreamBusy = new AutoResetEvent(true);

        private void BeginReading()
        {
            try
            {
                if (!(_bufferingMode is FrameBuffering))
                    _readerSocketStreamBusy.WaitOne();

                var rh = _socketStream.BeginRead(_buffer[_bufNo], 0, _buffer[_bufNo].Length, new AsyncCallback((ar) =>
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
                            foreach (var frame in _bufferingMode.Process(_buffer[_bufNo], bytesReadCount, _socketStream, _compressor))
                            {
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

                                    throw new InvalidOperationException("Protocol error! Unmached response. Terminating all requests now...");
                                }

                                act.BeginInvoke(frame, (tar) =>
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
                }), null);
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

        private void ForceComplete(Exception ex=null)
        {
            for (int streamId = 0; streamId < sbyte.MaxValue + 1; streamId++)
            {
                if (_frameReadTimers[streamId] != null)
                    _frameReadTimers[streamId].Change(Timeout.Infinite, Timeout.Infinite);
                var ar = _frameReadAsyncResult[streamId];
                if (ar != null && !ar.IsCompleted)
                    _frameReadAsyncResult[streamId].Complete(ex ?? new CassandraConnectionIOException());
            }
        }

        private bool SetupSocketException(Exception ex)
        {
            ForceComplete(ex);

            HostIsDown();
            try { _bufferingMode.Close(); }
            catch { }

            _socketExceptionOccured.TryTake();
            return (ex.InnerException != null && IsStreamRelatedException(ex.InnerException)) || IsStreamRelatedException(ex);
        }

        BoolSwitch _alreadyDisposed = new BoolSwitch();

        void CheckDisposed()
        {
            if (_alreadyDisposed.IsTaken())
                throw new ObjectDisposedException("CassandraConnection");
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

        ~CassandraConnection()
        {
            Dispose();
        }

        private void Evaluate(IRequest req, int streamId, Action<ResponseFrame> nextAction)
        {
            try
            {
                var frame = req.GetFrame();
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
            catch (Exception ex)
            {
                if (!SetupSocketException(ex))
                    throw;
            }
        }

        internal IPAddress GetHostAdress()
        {
            return _serverAddress;
        }
    }
}
