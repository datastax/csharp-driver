using System;
using System.Threading;

namespace Cassandra
{
    internal class AtomicArray<T>
    {
        readonly T[] _arr = null;
        public AtomicArray(int size)
        {
            _arr = new T[size];
            Thread.MemoryBarrier();
        }
        public T this[int idx]
        {
            get
            {
                Thread.MemoryBarrier();
                var r = this._arr[idx];
                Thread.MemoryBarrier();
                return r;
            }
            set
            {
                Thread.MemoryBarrier();
                _arr[idx] = value;
                Thread.MemoryBarrier();
            }
        }

        public void BlockCopyFrom(T[] src, int srcoffset, int destoffset, int count)
        {
            Thread.MemoryBarrier();
            Buffer.BlockCopy(src, srcoffset, _arr, destoffset, count); 
            Thread.MemoryBarrier();
        }
    }
}