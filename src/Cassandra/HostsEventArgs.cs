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