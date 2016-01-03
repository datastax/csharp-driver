﻿using System;
using System.Collections;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace Cassandra.Collections
{
    /// <summary>
    /// A thread-safe variant of List{T} in which all mutative operations (Add and Remove) are implemented by making a copy of the underlying array.
    /// </summary>
    internal class CopyOnWriteList<T> : ICollection<T>
    {
        private static readonly T[] Empty = new T[0];

        private volatile T[] _array;
        private readonly object _writeLock = new object();

        public int Count { get { return _array.Length; } }

        public bool IsReadOnly { get { return false; } }

        internal CopyOnWriteList()
        {
            _array = Empty;
        }

        public IEnumerator<T> GetEnumerator()
        {
            return ((IEnumerable<T>)_array).GetEnumerator();
        }

        IEnumerator IEnumerable.GetEnumerator()
        {
            return GetEnumerator();
        }

        public void Add(T item)
        {
            lock (_writeLock)
            {
                var currentArray = _array;
                var newArray = new T[currentArray.Length + 1];
                currentArray.CopyTo(newArray, 0);
                //Add the new item at the end
                newArray[currentArray.Length] = item;
                _array = newArray;
            }
        }

        public void AddRange(T[] items)
        {
            if (items == null || items.Length == 0)
            {
                return;
            }
            lock (_writeLock)
            {
                var currentArray = _array;
                var newArray = new T[currentArray.Length + items.Length];
                currentArray.CopyTo(newArray, 0);
                //Add the new items at the end
                for (var i = 0; i < items.Length; i++)
                {
                    newArray[currentArray.Length + i] = items[i];
                }
                _array = newArray;
            }
        }

        public void Clear()
        {
            ClearAndGet();
        }

        /// <summary>
        /// Removes all items and returns the existing items as an atomic operation.
        /// </summary>
        public T[] ClearAndGet()
        {
            lock (_writeLock)
            {
                var items = _array;
                _array = Empty;
                return items;
            }
        }

        public bool Contains(T item)
        {
            return Array.IndexOf(_array, item) >= 0;
        }

        public void CopyTo(T[] array, int arrayIndex)
        {
            _array.CopyTo(array, arrayIndex);
        }

        public bool Remove(T item)
        {
            lock (_writeLock)
            {
                var index = Array.IndexOf(_array, item);
                if (index < 0)
                {
                    return false;
                }
                if (_array.Length == 1 && index == 0)
                {
                    //Do not allocate an extra array
                    _array = Empty;
                    return true;
                }
                var currentArray = _array;
                var newArray = new T[currentArray.Length - 1];
                if (index != 0)
                {
                    Array.Copy(currentArray, 0, newArray, 0, index);
                }
                if (index != currentArray.Length - 1)
                {
                    Array.Copy(currentArray, index + 1, newArray, index, currentArray.Length - index - 1);
                }
                _array = newArray;
            }
            return true;
        }

        /// <summary>
        /// Gets a reference to the inner array
        /// </summary>
        public T[] GetSnapshot()
        {
            return _array;
        }
    }
}
