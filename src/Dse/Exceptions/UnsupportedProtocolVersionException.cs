//
//  Copyright (C) DataStax, Inc.
//
//  Please see the license for details:
//  http://www.datastax.com/terms/datastax-dse-driver-license-terms
//

using System;

namespace Dse
{
    internal class UnsupportedProtocolVersionException : DriverException
    {
        /// <summary>
        /// The version that was not supported by the server.
        /// </summary>
        public ProtocolVersion ProtocolVersion { get; }

        public UnsupportedProtocolVersionException(ProtocolVersion protocolVersion, Exception innerException) : 
            base(string.Format("Protocol version {0} not supported", protocolVersion), innerException)
        {
            ProtocolVersion = protocolVersion;
        }
    }
}
