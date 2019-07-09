//
//  Copyright (C) DataStax, Inc.
//
//  Please see the license for details:
//  http://www.datastax.com/terms/datastax-dse-driver-license-terms
//

using System.Net;

namespace Dse
{
    /// <summary>
    /// This class contains properties related to the proxy when using SNI.
    /// </summary>
    internal class SniOptions
    {
        public SniOptions(IPAddress ip, int port, string name)
        {
            Ip = ip;
            Port = port;
            Name = name;
        }

        public IPAddress Ip { get; }

        public string Name { get; }

        public int Port { get; }

        public bool IsIp => Ip != null;
    }
}