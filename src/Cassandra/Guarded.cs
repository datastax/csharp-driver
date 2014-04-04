using System.Threading;

namespace Cassandra
{
    internal class Guarded<T>
    {
        private T _val;

        public T Value
        {
            get
            {
                AssureLocked();
                return _val;
            }
            set
            {
                AssureLocked();
                _val = value;
            }
        }

        public Guarded(T val)
        {
            _val = val;
            Thread.MemoryBarrier();
        }

        private void AssureLocked()
        {
            if (Monitor.TryEnter(this))
                Monitor.Exit(this);
            else
                throw new SynchronizationLockException();
        }
    }
}