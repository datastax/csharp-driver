//
//  Copyright (C) 2017 DataStax, Inc.
//
//  Please see the license for details:
//  http://www.datastax.com/terms/datastax-dse-driver-license-terms
//

using System;

namespace Dse.Data.Linq
{
    public class CqlArgumentException : ArgumentException
    {
        internal CqlArgumentException(string message)
            : base(message)
        {
        }
    }
}