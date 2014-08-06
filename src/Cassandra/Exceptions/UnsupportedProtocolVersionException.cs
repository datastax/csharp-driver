using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace Cassandra
{
    internal class UnsupportedProtocolVersionException : DriverException
    {
        public UnsupportedProtocolVersionException(int protocolVersion, Exception innerException) : 
            base(String.Format("Protocol version {0} not supported", protocolVersion), innerException)
        {
                
        }
    }
}
