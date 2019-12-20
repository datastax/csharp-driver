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

        /// <summary>
        /// The version with which the server replied.
        /// </summary>
        public ProtocolVersion ResponseProtocolVersion { get; }

        public UnsupportedProtocolVersionException(ProtocolVersion protocolVersion, ProtocolVersion responseProtocolVersion, Exception innerException) :
            base($"Protocol version {protocolVersion} not supported", innerException)
        {
            ProtocolVersion = protocolVersion;
            ResponseProtocolVersion = responseProtocolVersion;
        }
    }
}
