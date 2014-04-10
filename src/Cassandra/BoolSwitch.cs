using System.Threading;

namespace Cassandra
{
    internal class BoolSwitch
    {
        private int _val;

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