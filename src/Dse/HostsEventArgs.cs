//
//  Copyright (C) 2017 DataStax, Inc.
//
//  Please see the license for details:
//  http://www.datastax.com/terms/datastax-dse-driver-license-terms
//

using System;
using System.Net;

namespace Cassandra
{
    public class HostsEventArgs : EventArgs
    {
        public enum Kind
        {
            Up,
            Down
        }

        public IPEndPoint Address;
        public Kind What;
    }
}