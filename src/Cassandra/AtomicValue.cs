using System.Threading;

namespace Cassandra
{
    internal class AtomicValue<T>
    {
        public T RawValue;

        public T Value
        {
            get
            {
                Thread.MemoryBarrier();
                T r = RawValue;
                Thread.MemoryBarrier();
                return r;
            }
            set
            {
                Thread.MemoryBarrier();
                RawValue = value;
                Thread.MemoryBarrier();
            }
        }

        public AtomicValue(T val)
        {
            RawValue = val;
            Thread.MemoryBarrier();
        }
    }
}