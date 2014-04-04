using System;
using System.Threading;

namespace Cassandra
{
    internal class AtomicArray<T>
    {
        private readonly T[] _arr;

        public T this[int idx]
        {
            get
            {
                Thread.MemoryBarrier();
                T r = _arr[idx];
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

        public AtomicArray(int size)
        {
            _arr = new T[size];
            Thread.MemoryBarrier();
        }

        public void BlockCopyFrom(T[] src, int srcoffset, int destoffset, int count)
        {
            Thread.MemoryBarrier();
            Buffer.BlockCopy(src, srcoffset, _arr, destoffset, count);
            Thread.MemoryBarrier();
        }
    }
}