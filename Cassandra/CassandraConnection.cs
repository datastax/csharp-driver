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
            _socket.Value.Shutdown(SocketShutdown.Both);
			}
			catch{}
        }
#endif
        readonly IPAddress _serverAddress;
        readonly int _port;
        readonly Guarded<Socket> _socket = new Guarded<Socket>(null);
        readonly Guarded<Stack<int>> _freeStreamIDs = new Guarded<Stack<int>>(new Stack<int>());
        readonly bool[] _freeStreamIDtaken = new bool[byte.MaxValue + 1];
        readonly AtomicValue<bool> _isStreamOpened = new AtomicValue<bool>(false);
        readonly int _queryAbortTimeout = Timeout.Infinite;
        readonly int _asyncCallAbortTimeout = Timeout.Infinite;

        readonly object _frameGuardier = new object();

        readonly AtomicValue<Action<ResponseFrame>> _frameEventCallback = new AtomicValue<Action<ResponseFrame>>(null);
        readonly AtomicArray<Action<ResponseFrame>> _frameReadCallback = new AtomicArray<Action<ResponseFrame>>(sbyte.MaxValue + 1);
        readonly AtomicArray<AsyncResult<IOutput>> _frameReadAsyncResult = new AtomicArray<AsyncResult<IOutput>>(sbyte.MaxValue + 1);
        readonly AtomicArray<Timer> _frameReadTimers = new AtomicArray<Timer>(sbyte.MaxValue + 1);

        Action<ResponseFrame> _defaultFatalErrorAction;

        struct ErrorActionParam
        {
            public AbstractResponse AbstractResponse;
            public int StreamId;
        }

        Action<ErrorActionParam> _protocolErrorHandlerAction;

        readonly IAuthInfoProvider _authInfoProvider;

        readonly Session _owner;

        private readonly SocketOptions _socketOptions;

        void HostIsDown()
        {
            _owner.HostIsDown(_serverAddress);
        }

        internal CassandraConnection(Session owner, IPAddress serverAddress, ProtocolOptions protocolOptions,
                                     SocketOptions socketOptions, ClientOptions clientOptions,
                                     IAuthInfoProvider authInfoProvider)
        {
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

            this._authInfoProvider = authInfoProvider;
            if (protocolOptions.Compression == CompressionType.Snappy)
            {
                _startupOptions.Add("COMPRESSION", "snappy");
                _compressor = new SnappyProtoBufCompressor();
            }
            this._serverAddress = serverAddress;
            this._port = protocolOptions.Port;
            this._queryAbortTimeout = clientOptions.QueryAbortTimeout;
            this._asyncCallAbortTimeout = clientOptions.AsyncCallAbortTimeout;

            this._socketOptions = socketOptions;

            CreateConnection();
            if (IsHealthy)
                BeginReading();
        }

        private void CreateConnection()
        {
            lock (_freeStreamIDs)
            {
                for (int i = 0; i <= sbyte.MaxValue; i++)
                {
                    _freeStreamIDs.Value.Push(i);
                    _freeStreamIDtaken[i] = false;
                }
            }

            _protocolErrorHandlerAction = new Action<ErrorActionParam>((param) =>
               {
                   if (param.AbstractResponse is ErrorResponse)
                       JobFinished(
                           param.StreamId,
                           (param.AbstractResponse as ErrorResponse).Output);
               });

            lock (_frameGuardier)
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

            newSock.Connect(new IPEndPoint(_serverAddress, _port));
            _socket.Value = newSock;
            _bufferingMode.Reset();
            _readerSocketStream = new NetworkStream(_socket.Value);
            _writerSocketStream = new NetworkStream(_socket.Value);
        }

        byte[][] _buffer = null;
        int _bufNo = 0;

        private NetworkStream _readerSocketStream;
        private NetworkStream _writerSocketStream;

        readonly IBuffering _bufferingMode;

        internal int AllocateStreamId()
        {
            lock (_freeStreamIDs)
            {
                if (_freeStreamIDs.Value.Count > 0)
                {
                    int i = _freeStreamIDs.Value.Pop();
                    _freeStreamIDtaken[i] = true;
                    return i;
                }
                else
                    throw new StreamAllocationException();
            }
        }

        private void FreeStreamId(int streamId)
        {
            lock (_freeStreamIDs)
            {
                if (!_freeStreamIDtaken[streamId])
                    return;
                _freeStreamIDtaken[streamId] = false;
                _freeStreamIDs.Value.Push(streamId);
            }
        }

        public bool IsBusy(int max)
        {
            lock (_freeStreamIDs)
                return sbyte.MaxValue + 1 - _freeStreamIDs.Value.Count >= max;
        }

        public bool IsFree(int min)
        {
            lock (_freeStreamIDs)
                return sbyte.MaxValue + 1 - _freeStreamIDs.Value.Count <= min;
        }

        public bool IsEmpty()
        {
            lock (_freeStreamIDs)
                return _freeStreamIDs.Value.Count == sbyte.MaxValue + 1;
        }

        private void JobFinished(int streamId, IOutput outp)
        {
            AsyncResult<IOutput> ar = null;
            try
            {
                lock (_frameGuardier)
                {
                    ar = _frameReadAsyncResult[streamId];
                    _frameReadAsyncResult[streamId] = null;
                    FreeStreamId(streamId);
                }
            }
            finally
            {
                if (ar != null)
                {
                    ar.SetResult(outp);
                    ar.Complete();
                    (outp as IWaitableForDispose).WaitForDispose();
                }
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
            _defaultFatalErrorAction = new Action<ResponseFrame>((frame2) =>
            {
                var response2 = FrameParser.Parse(frame2);
                _protocolErrorHandlerAction(new ErrorActionParam() { AbstractResponse = response2, StreamId = streamId });
            });

            var ar = new AsyncResult<IOutput>(streamId, callback, state, owner, propId, null, null, _asyncCallAbortTimeout);

            lock (_frameGuardier)
                _frameReadAsyncResult[streamId] = ar;

            return ar;
        }

        private void BeginJob(AsyncResult<IOutput> jar,  Action<int> job, bool startup = true)
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
                            job(jar.StreamId);
                        }
                        else if (response is AuthenticateResponse)
                        {
                            if (_authInfoProvider == null)
                                throw new AuthenticationException("Credentials are required.", _serverAddress);

                            var credentials = _authInfoProvider.GetAuthInfos(_serverAddress);

                            Evaluate(new CredentialsRequest(jar.StreamId, credentials), jar.StreamId, new Action<ResponseFrame>((frame2) =>
                            {
                                var response2 = FrameParser.Parse(frame2);
                                if (response2 is ReadyResponse)
                                {
                                    _isStreamOpened.Value = true;
                                    job(jar.StreamId);
                                }
                                else
                                    _protocolErrorHandlerAction(new ErrorActionParam() { AbstractResponse = response2, StreamId = jar.StreamId });
                            }));
                        }
                        else
                            _protocolErrorHandlerAction(new ErrorActionParam() { AbstractResponse = response, StreamId = jar.StreamId });
                    });
                }
                else
                    job(jar.StreamId);
            }
            catch (Exception ex)
            {
                if (!SetupSocketException(ex))
                    throw;
            }
        }

        readonly IProtoBufComporessor _compressor = null;

        private bool _socketExceptionOccured = false;

        public bool IsHealthy
        {
            get
            {
                lock (_statusGuardier)
                {
                    return !_alreadyDisposed && !_socketExceptionOccured;
                }
            }
        }
        private void AbortTimerProc(object state)
        {
            int streamId = (int)state;

            SetupSocketException(new CassandraConnectionTimeoutException());

            lock (_socket)
                if (_socket.Value != null)
                {
                    try
                    {
                        _readerSocketStream.Close();
                        _writerSocketStream.Close();
                        _socket.Value.Shutdown(SocketShutdown.Both);
                        _socket.Value.Disconnect(_socketOptions.ReuseAddress ?? false);
                    }
                    catch (Exception ex)
                    {
                        if (!IsStreamRelatedException(ex))
                            throw;
                    }
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

        readonly Guarded<bool> _readerSocketStreamBusy = new Guarded<bool>(false);

        private void BeginReading()
        {
            try
            {
                if (!(_bufferingMode is FrameBuffering))
                    lock (_readerSocketStreamBusy)
                    {
                        while (_readerSocketStreamBusy.Value)
                            Monitor.Wait(_readerSocketStreamBusy);
                        _readerSocketStreamBusy.Value = true;
                    }

                lock (_readerSocketStream)
                {
                    var rh = _readerSocketStream.BeginRead(_buffer[_bufNo], 0, _buffer[_bufNo].Length, new AsyncCallback((ar) =>
                    {

                        try
                        {
                            int bytesReadCount;
                            lock (_readerSocketStream)
                                bytesReadCount = _readerSocketStream.EndRead(ar);

                            if (bytesReadCount == 0)
                            {
                                if (_alreadyDisposed)
                                {
                                    CompleteReader();
                                    return;
                                }

                                throw new CassandraConnectionIOException();
                            }
                            else
                            {
                                foreach (var frame in _bufferingMode.Process(_buffer[_bufNo], bytesReadCount, _readerSocketStream, _compressor))
                                {
                                    Action<ResponseFrame> act = null;
                                    lock (_frameGuardier)
                                    {
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
                            {
                                lock (_readerSocketStreamBusy)
                                {
                                    _readerSocketStreamBusy.Value = false;
                                    Monitor.PulseAll(_readerSocketStreamBusy);
                                }
                            }
                        }
                    }), null);
                }
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

        private void CompleteReader()
        {
            var toCompl = new List<AsyncResult<IOutput>>();
            try
            {
                lock (_frameGuardier)
                {
                    for (int streamId = 0; streamId < sbyte.MaxValue + 1; streamId++)
                        if (_frameReadAsyncResult[streamId] != null)
                        {
                            toCompl.Add(_frameReadAsyncResult[streamId]);
                            FreeStreamId(streamId);
                        }
                }
            }
            finally
            {
                foreach (var ar in toCompl)
                    if (!ar.IsCompleted)
                        ar.Complete(new CassandraConnectionIOException());
            }
        }

        private bool SetupSocketException(Exception ex)
        {
            var toCompl = new List<AsyncResult<IOutput>>();
            try
            {
                lock (_frameGuardier)
                {
                    for (int streamId = 0; streamId < sbyte.MaxValue + 1; streamId++)
                    {
                        if (_frameReadTimers[streamId] != null)
                            _frameReadTimers[streamId].Change(Timeout.Infinite, Timeout.Infinite);
                        if (_frameReadAsyncResult[streamId] != null)
                        {
                            toCompl.Add(_frameReadAsyncResult[streamId]);
                            FreeStreamId(streamId);
                        }
                    }
                    HostIsDown();
                    try { _bufferingMode.Close(); }
                    catch { }

                    lock (_statusGuardier)
                        _socketExceptionOccured = true;
                }
                return (ex.InnerException != null && IsStreamRelatedException(ex.InnerException)) || IsStreamRelatedException(ex);
            }
            finally
            {
                foreach (var ar in toCompl)
                    if(!ar.IsCompleted)
                        ar.Complete(ex);
            }
        }

        readonly object _statusGuardier = new object();
        bool _alreadyDisposed = false;

        void CheckDisposed()
        {
            lock (_statusGuardier)
                if (_alreadyDisposed)
                    throw new ObjectDisposedException("CassandraConnection");
        }

        public void Dispose()
        {
            lock (_statusGuardier)
            {
                if (_alreadyDisposed)
                    return;

                _alreadyDisposed = true;

                lock (_socket)
                    if (_socket.Value != null)
                    {
                        try
                        {
                            _socket.Value.Shutdown(SocketShutdown.Both);
                            _socket.Value.Disconnect(_socketOptions.ReuseAddress ?? false);
                        }
                        catch (ObjectDisposedException)
                        {
                        }
                        catch (SocketException)
                        {
                        }
                    }
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
                lock (_writerSocketStream)
                {
                    lock (_frameGuardier)
                    {
                        _frameReadCallback[streamId] = nextAction;
                        if (_queryAbortTimeout != Timeout.Infinite)
                        {
                            if (_frameReadTimers[streamId] == null)
                                _frameReadTimers[streamId] = new Timer(AbortTimerProc, streamId, _queryAbortTimeout, Timeout.Infinite);
                            else
                                _frameReadTimers[streamId].Change(_queryAbortTimeout, Timeout.Infinite);
                        }
                    }
                    var wr = _writerSocketStream.BeginWrite(frame.Buffer, 0, frame.Buffer.Length, null, null);
                    _writerSocketStream.EndWrite(wr);
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
