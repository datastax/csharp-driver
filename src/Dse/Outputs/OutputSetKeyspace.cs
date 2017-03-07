//
//  Copyright (C) 2017 DataStax, Inc.
//
//  Please see the license for details:
//  http://www.datastax.com/terms/datastax-dse-driver-license-terms
//

using System;

namespace Dse
{
    internal class OutputSetKeyspace : IOutput
    {
        public string Value { get; set; }

        public Guid? TraceId { get; internal set; }

        internal OutputSetKeyspace(string val)
        {
            Value = val;
        }

        public void Dispose()
        {
        }
    }
}
