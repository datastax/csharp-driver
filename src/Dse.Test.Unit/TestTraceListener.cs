//
//       Copyright (C) 2019 DataStax, Inc.
//
//     Please see the license for details:
//     http://www.datastax.com/terms/datastax-dse-driver-license-terms
//

using System.Collections.Concurrent;
using System.Diagnostics;

namespace Dse.Test.Unit
{
    internal class TestTraceListener : TraceListener
    {
        public ConcurrentQueue<string> Queue = new ConcurrentQueue<string>();

        public override void Write(string message)
        {
            Queue.Enqueue(message);
        }

        public override void WriteLine(string message)
        {
            Queue.Enqueue(message);
        }
    }
}