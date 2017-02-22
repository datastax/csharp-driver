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
    /// <summary>
    /// Exception that indicates that the request is not valid.
    /// </summary>
    public class RequestInvalidException : DriverException
    {
        public RequestInvalidException(string message) : base(message)
        {

        }
    }
}
