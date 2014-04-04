using System.Threading;

namespace Cassandra
{
    internal class BoolSwitch
    {
        private int val;

        public bool TryTake()
        {
            return Interlocked.Increment(ref val) == 1;
        }

        public bool IsTaken()
        {
            return val > 0;
        }
    }
}