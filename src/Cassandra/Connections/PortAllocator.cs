using System;
using System.Net;
using System.Net.Sockets;
using System.Threading;

static class PortAllocator
{
    private static int lastPort = -1;

    public static int GetNextAvailablePort(int shardCount, int shardId, int lowPort, int highPort)
    {
        int foundPort = -1;
        int lastPortValue;

        do
        {
            lastPortValue = Volatile.Read(ref lastPort);

            int scanStart = lastPortValue == -1 ? lowPort : lastPortValue;
            if (scanStart < lowPort)
            {
                scanStart = lowPort;
            }

            scanStart += (shardCount - scanStart % shardCount) + shardId;

            for (int port = scanStart; port <= highPort; port += shardCount)
            {
                if (IsTcpPortAvailable(port))
                {
                    foundPort = port;
                    break;
                }
            }

            if (foundPort == -1)
            {
                scanStart = lowPort + (shardCount - lowPort % shardCount) + shardId;

                for (int port = scanStart; port <= highPort; port += shardCount)
                {
                    if (IsTcpPortAvailable(port))
                    {
                        foundPort = port;
                        break;
                    }
                }
            }

            if (foundPort == -1)
            {
                return -1;
            }
        }
        while (Interlocked.CompareExchange(ref lastPort, foundPort, lastPortValue) != lastPortValue);

        return foundPort;
    }

    public static bool IsTcpPortAvailable(int port)
    {
        try
        {
            TcpListener listener = new TcpListener(IPAddress.Loopback, port);
            listener.Start();
            listener.Stop();
            return true;
        }
        catch (SocketException)
        {
            return false;
        }
    }
}
