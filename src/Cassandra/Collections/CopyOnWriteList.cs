//
//      Copyright (C) DataStax Inc.
//
//   Licensed under the Apache License, Version 2.0 (the "License");
//   you may not use this file except in compliance with the License.
//   You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
//   Unless required by applicable law or agreed to in writing, software
//   distributed under the License is distributed on an "AS IS" BASIS,
//   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//   See the License for the specific language governing permissions and
//   limitations under the License.
//

using System;
using System.Collections;
using System.Collections.Generic;

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

        /// <summary>
        /// Adds a new item to the list
        /// </summary>
        public void Add(T item)
        {
            AddNew(item);
        }

        /// <summary>
        /// Adds a new item to the list and returns the new length
        /// </summary>
        public int AddNew(T item)
        {
            lock (_writeLock)
            {
                var currentArray = _array;
                var newArray = new T[currentArray.Length + 1];
                currentArray.CopyTo(newArray, 0);
                // Add the item at the end
                newArray[currentArray.Length] = item;
                _array = newArray;
                return newArray.Length;
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
            return RemoveAndCount(item).Item1;
        }

        /// <summary>
        /// Removes the item and returns the a boolean that determines if the item has been removed and an integer with the
        /// new count, as an atomic operation.
        /// </summary>
        public Tuple<bool, int> RemoveAndCount(T item)
        {
            lock (_writeLock)
            {
                var index = Array.IndexOf(_array, item);
                if (index < 0)
                {
                    return Tuple.Create(false, _array.Length);
                }
                if (_array.Length == 1 && index == 0)
                {
                    //Do not allocate an extra array
                    _array = Empty;
                    return Tuple.Create(true, 0);
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
                return Tuple.Create(true, newArray.Length);
            }
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
