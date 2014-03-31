using System.Threading;

namespace Cassandra
{
    internal class Guarded<T>
    {
        T _val;

        void AssureLocked()
        {
            if (Monitor.TryEnter(this))
                Monitor.Exit(this);
            else
                throw new System.Threading.SynchronizationLockException();
        }
        
        public Guarded(T val)
        {
            this._val = val;
            Thread.MemoryBarrier();
        }
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
    }
}