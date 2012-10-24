using System;
using System.Collections.Generic;
using System.Text;
using System.Net;
using System.Net.Sockets;
using System.IO;
using System.Threading;
using System.Diagnostics;

namespace Cassandra
{
    public delegate IDictionary<string, string> CredentialsDelegate(string Authenticator);
    public enum CassandraCompressionType { NoCompression, Snappy }
}

namespace Cassandra.Native
{
    internal enum BufferingMode { NoBuffering, FrameBuffering }

    internal partial class CassandraConnection 
    {

        EndPoint serverAddress;
        Guarded<Socket> socket = new Guarded<Socket>(null);
        Guarded<Stack<int>> freeStreamIDs = new Guarded<Stack<int>>(new Stack<int>());
        AtomicValue<bool> isStreamOpened = new AtomicValue<bool>(false);

        AtomicValue<Action<ResponseFrame>> frameEventCallback = new AtomicValue<Action<ResponseFrame>>(null);
        AtomicArray<Action<ResponseFrame>> frameReadCallback = new AtomicArray<Action<ResponseFrame>>(sbyte.MaxValue + 1);
        AtomicArray<AsyncResult<IOutput>> frameReadAsyncResult = new AtomicArray<AsyncResult<IOutput>>(sbyte.MaxValue + 1);

        Action<ResponseFrame> defaultFatalErrorAction;

        struct ErrorActionParam
        {
            public AsyncResult<IOutput> AsyncResult;
            public IResponse Response;
            public int streamId;
        }

        Action<ErrorActionParam> ProtocolErrorHandlerAction;

        CredentialsDelegate credentialsDelegate;

        internal CassandraConnection(IPEndPoint serverAddress, CredentialsDelegate credentialsDelegate = null, CassandraCompressionType compression = CassandraCompressionType.NoCompression, int abortTimeout = Timeout.Infinite)
        {
            bufferingMode = null;
            switch (compression)
            {
                case CassandraCompressionType.Snappy:
                    bufferingMode = new FrameBuffering();
                    break;
                case CassandraCompressionType.NoCompression:
                    bufferingMode = new NoBuffering();
                    break;
                default:
                    throw new InvalidOperationException();
            }

            this.credentialsDelegate = credentialsDelegate;
            if (compression == CassandraCompressionType.Snappy)
            {
                startupOptions.Add("COMPRESSION", "snappy");
                compressor = new SnappyProtoBufCompressor();
            }
            this.serverAddress = serverAddress;
            this.abortTimeout = abortTimeout;
            abortTimer = new Timer(abortTimerProc, null, Timeout.Infinite, Timeout.Infinite);
            createConnection();
            again();
        }

        private void createConnection()
        {
            lock (freeStreamIDs)
            {
                for (int i = 0; i <= sbyte.MaxValue; i++)
                    freeStreamIDs.Value.Push(i);
            }

            ProtocolErrorHandlerAction = new Action<ErrorActionParam>((param) =>
               {
                   if (param.Response is ErrorResponse)
                       JobFinished(param.AsyncResult,
                           param.streamId,
                           (param.Response as ErrorResponse).Output);
               });

            frameEventCallback.Value = new Action<ResponseFrame>(EventOccured);

            buffer = new byte[][] { 
                    new byte[bufferingMode.PreferedBufferSize()], 
                    new byte[bufferingMode.PreferedBufferSize()] };

            var newSock = new Socket(AddressFamily.InterNetwork, SocketType.Stream, ProtocolType.Tcp);
            
            newSock.Connect(serverAddress);
            socket.Value = newSock;
            bufferingMode.Reset();
            readerSocketStream = new NetworkStream(socket.Value);
        }

        byte[][] buffer = null;
        int bufNo = 0;

        private NetworkStream readerSocketStream;

        IBuffering bufferingMode;

        private int allocateStreamId()
        {
            lock (freeStreamIDs)
            {
                while (true)
                    if (freeStreamIDs.Value.Count > 0)
                        return freeStreamIDs.Value.Pop();
                    else
                        return -1;
            }
        }

        private void freeStreamId(int streamId)
        {
            lock (freeStreamIDs)
            {
                freeStreamIDs.Value.Push(streamId);
            }
        }

        private void JobFinished(AsyncResult<IOutput> ar, int streamId, IOutput outp)
        {
            frameReadAsyncResult[streamId] = null;
            ar.SetResult(outp);
            ar.Complete();
            freeStreamId(streamId);
            (outp as IWaitableForDispose).WaitForDispose();
        }

        private NetworkStream CreateSocketStream()
        {
            checkDisposed();
            return new NetworkStream(socket.Value);
        }

        Dictionary<string, string> startupOptions = new Dictionary<string, string>()
        {
            {"CQL_VERSION","3.0.0"}
        };

        static FrameParser FrameParser = new FrameParser();

        internal class StreamAllocationException : Exception
        {
        }

        private void BeginJob(AsyncResult<IOutput> ar, Action<int> job, NetworkStream socketStream, bool startup = true)
        {
            var streamId = allocateStreamId();
            if (streamId == -1)
                throw new StreamAllocationException();

            defaultFatalErrorAction = new Action<ResponseFrame>((frame2) =>
            {
                var response2 = FrameParser.Parse(frame2);
                ProtocolErrorHandlerAction(new ErrorActionParam() { AsyncResult = ar, Response = response2, streamId = streamId });
            });

            try
            {
                if (startup && !isStreamOpened.Value)
                {
                    Evaluate(new StartupRequest(streamId, startupOptions), ar, streamId, (frame) =>
                    {
                        var response = FrameParser.Parse(frame);
                        if (response is ReadyResponse)
                        {
                            isStreamOpened.Value = true;
                            job(streamId);
                        }
                        else if (response is AuthenticateResponse)
                        {
                            if (credentialsDelegate == null)
                                throw new InvalidOperationException();

                            var credentials = credentialsDelegate((response as AuthenticateResponse).Authenticator);

                            Evaluate(new CredentialsRequest(streamId, credentials), ar, streamId, new Action<ResponseFrame>((frame2) =>
                            {
                                var response2 = FrameParser.Parse(frame2);
                                if (response2 is ReadyResponse)
                                {
                                    isStreamOpened.Value = true;
                                    job(streamId);
                                }
                                else
                                    ProtocolErrorHandlerAction(new ErrorActionParam() { AsyncResult = ar, Response = response2, streamId = streamId });
                            }), socketStream);
                        }
                        else
                            ProtocolErrorHandlerAction(new ErrorActionParam() { AsyncResult = ar, Response = response, streamId = streamId });
                    }, socketStream);
                }
                else
                    job(streamId);
            }
            catch (Exception ex)
            {
                Debug.WriteLine(ex.Message, "CassandraConnection.BeginJob");
                if (setupWriterException(ex, streamId, ar))
                    writerSocketExceptionOccured = true;
                else
                    throw;
            }
        }

        IProtoBufComporessor compressor = null;

        private bool readerSocketExceptionOccured = false;
        private bool writerSocketExceptionOccured = false;

        public bool IsHealthy
        {
            get
            {
                lock (alreadyDisposed)
                    return !alreadyDisposed.Value && !readerSocketExceptionOccured && !writerSocketExceptionOccured;
            }
        }

        Timer abortTimer =null;

        int abortTimeout = Timeout.Infinite;

        Guarded<bool> processed = new Guarded<bool>(false);

        private void abortTimerProc(object _)
        {
            lock(processed)
            {
                if(processed.Value)
                    return;
                processed.Value = true;
            }

            lock (socket)
                if (socket.Value != null)
                {
                    try
                    {
                        readerSocketStream.Close();
                        socket.Value.Shutdown(SocketShutdown.Both);
                        socket.Value.Disconnect(false);
                    }
                    catch (ObjectDisposedException ex)
                    {
                        Debug.WriteLine(ex.Message, "CassandraConnection.abortTimer1");
                    }
                    catch (SocketException ex)
                    {
                        Debug.WriteLine(ex.Message, "CassandraConnection.abortTimer2");
                    }
                }
            if (setupReaderException(new IOCassandraException()))
            {
                try
                {
                    bufferingMode.Close();
                }
                catch (IOCassandraException)
                {
                }
                readerSocketExceptionOccured = true;
                again();
            }
        }        

        internal void BeginReading()
        {
            lock (alreadyDisposed)
                if (alreadyDisposed.Value)
                    return;
            try
            {
                if (abortTimeout != Timeout.Infinite)
                {
                    lock (processed)
                        processed.Value = false;
                    abortTimer.Change(abortTimeout, Timeout.Infinite);
                }
                var rh = readerSocketStream.BeginRead(buffer[bufNo], 0, buffer[bufNo].Length, new AsyncCallback((ar) =>
                {
                    if (abortTimeout != Timeout.Infinite)
                    {
                        lock (processed)
                        {
                            if (processed.Value)
                                return;
                            processed.Value = true;
                        }
                        abortTimer.Change(Timeout.Infinite, Timeout.Infinite);
                    }

                    Debug.WriteLine("!readerSocketStream.BeginRead");
                    try
                    {
                        var bytesReadCount = readerSocketStream.EndRead(ar);
                        if (bytesReadCount == 0)
                        {
                            Debug.WriteLine("!readerSocketStream.0");
                            readerSocketExceptionOccured = true;
                        }
                        else
                        {
                            foreach (var frame in bufferingMode.Process(buffer[bufNo], bytesReadCount, readerSocketStream, compressor))
                            {
                                Action<ResponseFrame> act = null;
                                if (frame.FrameHeader.streamId == 0xFF)
                                    act = frameEventCallback.Value;
                                else if(frame.FrameHeader.streamId>=0)
                                    act = frameReadCallback[frame.FrameHeader.streamId];

                                if (act == null)
                                    act = defaultFatalErrorAction; //TODO: what to do here? this is a protocol valiation

                                act.BeginInvoke(frame, (tar) =>
                                {
                                    Debug.WriteLine("! act.BeginInvoke");
                                    try
                                    {
                                        (tar.AsyncState as Action<ResponseFrame>).EndInvoke(tar);
                                        Debug.WriteLine("! act.EndInvoke");
                                    }
                                    catch (Exception ex)
                                    {
                                        Debug.WriteLine(ex.Message, "CassandraConnection.BeginReading");
                                        if (setupReaderException(ex))
                                        {
                                            bufferingMode.Close();
                                            readerSocketExceptionOccured = true;
                                        }
                                    }
                                    finally
                                    {
                                        if (!(bufferingMode is FrameBuffering))
                                            again();
                                    }
                                }, act);

                            }
                            bufNo = 1 - bufNo;
                        }
                    }
                    catch (Exception ex)
                    {
                        Debug.WriteLine(ex.Message, "CassandraConnection.BeginReading2");
                        if (setupReaderException(ex))
                        {
                            bufferingMode.Close();
                            readerSocketExceptionOccured = true;
                        }
                        if (!(bufferingMode is FrameBuffering))
                            again();
                    }
                    finally
                    {
                        if (bufferingMode is FrameBuffering)
                            again();
                    }
                }), null);

            }
            catch (IOException e)
            {
                Debug.WriteLine(e.Message, "CassandraConnection.BeginReading3");
                if (setupReaderException(e))
                {
                    bufferingMode.Close();
                    readerSocketExceptionOccured = true;
                }
                else
                    throw;
            }
        }

        private bool setupReaderException(Exception ex)
        {
            for (int streamId = 0; streamId < sbyte.MaxValue + 1; streamId++)
                if (frameReadAsyncResult[streamId] != null)
                {
                    frameReadAsyncResult[streamId].Complete(ex);
                    isStreamOpened.Value = false;
                    freeStreamId(streamId);
                }
            return (ex.InnerException != null && ex.InnerException is SocketException) || ex is IOCassandraException;
        }

        Guarded<bool> alreadyDisposed = new Guarded<bool>(false);

        private bool setupWriterException(Exception ex, int streamId, AsyncResult<IOutput> ar)
        {
            ar.Complete(ex);
            freeStreamId(streamId);
            return (ex.InnerException != null && ex.InnerException is SocketException) || ex is IOCassandraException;
        }

        void checkDisposed()
        {
            lock (alreadyDisposed)
                if (alreadyDisposed.Value)
                    throw new ObjectDisposedException("CassandraConnection");
        }

        public void Dispose()
        {
            lock (alreadyDisposed)
            {
                if (alreadyDisposed.Value)
                    return;

                lock (socket)
                    if (socket.Value != null)
                    {
                        try
                        {
                            socket.Value.Shutdown(SocketShutdown.Both);
                            socket.Value.Disconnect(false);
                        }
                        catch (ObjectDisposedException ex)
                        {
                            Debug.WriteLine(ex.Message, "CassandraConnection.Dispose1");
                        }
                        catch (SocketException ex)
                        {
                            Debug.WriteLine(ex.Message, "CassandraConnection.Dispose2");
                        }
                    }

                alreadyDisposed.Value = true;
            }
        }

        private void again()
        {
            if (IsHealthy)
            {
                BeginReading();
            }
            else
            {
                Debug.WriteLine("!!!!");
            }
        }


        ~CassandraConnection()
        {
            Dispose();
        }

        private void Evaluate(IRequest req, AsyncResult<IOutput> ar, int streamId, Action<ResponseFrame> nextAction, NetworkStream socketStream)
        {
            try
            {
                frameReadCallback[streamId] = nextAction;
                frameReadAsyncResult[streamId] = ar;

                var frame = req.GetFrame();
                lock (socketStream)
                {
                    socketStream.Write(frame.buffer, 0, frame.buffer.Length);
                    socketStream.Flush();
                }
            }
            catch (Exception ex)
            {
                Debug.WriteLine(ex.Message, "CassandraConnection.Evaluate");
                if (setupWriterException(ex, streamId, ar))
                    writerSocketExceptionOccured = true;
            }
        }
    }
}
