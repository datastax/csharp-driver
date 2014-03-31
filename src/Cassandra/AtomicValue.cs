using System.Threading;

namespace Cassandra
{
    internal class AtomicValue<T> 
    {
        public T RawValue;
        public AtomicValue(T val)
        {
            this.RawValue = val;
            Thread.MemoryBarrier();
        }
        public T Value
        {
            get
            {
                Thread.MemoryBarrier();
                var r = this.RawValue;
                Thread.MemoryBarrier();
                return r;
            }
            set
            {
                Thread.MemoryBarrier();
                this.RawValue = value;
                Thread.MemoryBarrier();
            }
        }

    }
}