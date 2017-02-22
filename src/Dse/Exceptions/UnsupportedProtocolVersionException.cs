//
//  Copyright (C) 2017 DataStax, Inc.
//
//  Please see the license for details:
//  http://www.datastax.com/terms/datastax-dse-driver-license-terms
//

﻿using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace Cassandra
{
    internal class UnsupportedProtocolVersionException : DriverException
    {
        /// <summary>
        /// The version that was not supported by the server.
        /// </summary>
        public ProtocolVersion ProtocolVersion { get; private set; }

        public UnsupportedProtocolVersionException(ProtocolVersion protocolVersion, Exception innerException) : 
            base(string.Format("Protocol version {0} not supported", protocolVersion), innerException)
        {
            ProtocolVersion = protocolVersion;
        }
    }
}
