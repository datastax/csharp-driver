//
//  Copyright (C) DataStax, Inc.
//
//  Please see the license for details:
//  http://www.datastax.com/terms/datastax-dse-driver-license-terms
//

using System.Threading;

namespace Cassandra
{
    internal class BoolSwitch
    {
        private int _val = 0;

        public bool TryTake()
        {
            return Interlocked.Increment(ref _val) == 1;
        }

        public bool IsTaken()
        {
            return _val > 0;
        }
    }
}