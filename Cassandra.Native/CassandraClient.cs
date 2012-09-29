using System;
using System.Collections.Generic;
using System.Text;
using System.Net;
using System.Threading;
using System.Net.Sockets;
using System.IO;
using System.Diagnostics;

namespace Cassandra.Native
{
    public class CassandraClient : IDisposable
    {
        public CassandraClient()
        {
        }

        public CassandraConnection Connect(IPEndPoint serverAddress, CredentialsDelegate credentialsDelegate = null, CassandraCompressionType compression = CassandraCompressionType.NoCompression, BufferingMode mode = BufferingMode.FrameBuffering, int abortTimeout = Timeout.Infinite)
        {
            checkDisposed();

            var newConn = new CassandraConnection(this, serverAddress, credentialsDelegate, compression, mode, abortTimeout);
            PulseReadyToRead(newConn);
            startDispatchThread();
            return newConn;
        }

        volatile int startedConnections = 0;

        Queue<WeakReference<CassandraConnection>> readyToRead = new Queue<WeakReference<CassandraConnection>>();

        Thread dispatchThread = null;
        internal object DispatchSync = new object();

        internal void PulseReadyToRead(CassandraConnection connection)
        {
            lock (DispatchSync)
            {
                if (connection != null)
                    readyToRead.Enqueue(new WeakReference<CassandraConnection>(connection));
                Monitor.PulseAll(DispatchSync);
            }
        }

        internal void CloseConnection(CassandraConnection connection)
        {
            bool lastOne = false;
            lock (DispatchSync)
            {
                lastOne = startedConnections == 1;
                startedConnections--;
                Monitor.PulseAll(DispatchSync);
            }
            if (lastOne)
                dispatchThread.Join();
        }

        private void startDispatchThread()
        {
            lock (DispatchSync)
            {
                startedConnections++;
                if (startedConnections > 1)
                    return;
            }

            dispatchThread = new Thread(new ThreadStart(threadProc));
            dispatchThread.IsBackground = true;
            dispatchThread.Start();
        }

        private void threadProc()
        {
            lock (DispatchSync)
            {
                while (true)
                {
                    if (startedConnections == 0)
                        return;

                    WeakReference<CassandraConnection> cc = new WeakReference<CassandraConnection>(null);
                    
                    if (readyToRead.Count == 0)
                    {
                        Monitor.Wait(DispatchSync);
                    }
                    else
                    {
                        cc = readyToRead.Dequeue();
                        if (cc.IsAlive)
                        {
                            if (cc.Value.IsHealthy)
                            {
                                cc.Value.BeginReading();
                            }
                            else
                            {
                                Debug.WriteLine("!!!!");
                            }
                        }
                    }
                }
            }
        }

        Wrapper<bool> alreadyDisposed = new Wrapper<bool>(false);

        void checkDisposed()
        {
            lock (alreadyDisposed)
                if (alreadyDisposed.Value)
                    throw new ObjectDisposedException("CassandraClient");
        }

        public void Dispose()
        {
            lock (alreadyDisposed)
            {
                alreadyDisposed.Value = true;

                lock (DispatchSync)
                {
                    if (startedConnections > 0)
                    {
                        throw new InvalidOperationException();
                    }
                }
            }
        }
    }
}
