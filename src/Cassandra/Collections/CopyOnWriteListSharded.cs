using System;
using System.Collections;
using System.Collections.Generic;
using System.Linq;

namespace Cassandra.Collections
{
    internal interface IShardable
    {
        int ShardID { get; }
    }

    internal class ShardedList<T> : IEnumerable<T> where T : IShardable
    {
        private static readonly T[] EmptyArray = new T[0];

        private readonly T[] _array;
        private readonly T[][] _arrayPerShard;

        internal ShardedList()
        {
            _array = EmptyArray;
            _arrayPerShard = new T[0][];
        }

        public ShardedList(T[] array)
        {
            if (array == null || array.Length == 0)
            {
                return;
            }

            var maxShardId = array.Select(item => item.ShardID).Concat(new[] { -1 }).Max();
            if (maxShardId < 0)
            {
                _array = array.Clone() as T[];
                _arrayPerShard = new T[0][];
                return;
            }
            _arrayPerShard = new T[maxShardId + 1][];
            _array = array.Clone() as T[];
            for (var i = 0; i <= maxShardId; i++)
            {
                _arrayPerShard[i] = EmptyArray;
            }

            foreach (var item in array)
            {
                var shardId = item.ShardID;
                if (shardId < 0)
                {
                    continue;
                }
                var shardArray = _arrayPerShard[shardId];
                var newShardArray = new T[shardArray.Length + 1];
                shardArray.CopyTo(newShardArray, 0);
                newShardArray[shardArray.Length] = item;
                _arrayPerShard[shardId] = newShardArray;
            }
        }

        public T this[int index] => _array[index];
        public IEnumerator<T> GetEnumerator() => ((IEnumerable<T>)_array).GetEnumerator();
        IEnumerator IEnumerable.GetEnumerator() => GetEnumerator();
        public T[] GetAllItems() => _array;
        public T[] GetItemsForShard(int shardId)
        {
            if (shardId < 0 || shardId >= _arrayPerShard.Length)
            {
                return EmptyArray;
            }
            return _arrayPerShard[shardId];
        }

        public T[][] GetPerShardSnapshot() => _arrayPerShard;

        public int Length => _array.Length;
        public int Count => _array.Length;
        public bool IsReadOnly => true;
    }

    internal class CopyOnWriteShardedList<T> : ICollection<T> where T : IShardable
    {
        private static readonly T[] EmptyArray = new T[0];
        private volatile ShardedList<T> _shardedList;
        private readonly object _writeLock = new object();

        internal CopyOnWriteShardedList()
        {
            _shardedList = new ShardedList<T>();
        }

        public int Count => _shardedList.Count;
        public int Length => _shardedList.Length;
        public bool IsReadOnly => false;

        public IEnumerator<T> GetEnumerator() => _shardedList.GetEnumerator();
        IEnumerator IEnumerable.GetEnumerator() => GetEnumerator();

        public void Add(T item)
        {
            AddNew(item);
        }

        public int AddNew(T item)
        {
            lock (_writeLock)
            {
                var current = _shardedList;
                var currentArray = current.GetAllItems();

                var newArray = new T[currentArray.Length + 1];
                currentArray.CopyTo(newArray, 0);
                newArray[currentArray.Length] = item;

                _shardedList = new ShardedList<T>(newArray);
                return newArray.Length;
            }
        }

        public void AddRange(T[] items)
        {
            if (items == null || items.Length == 0)
                return;

            lock (_writeLock)
            {
                var current = _shardedList;
                var currentArray = current.GetAllItems();

                var newArray = new T[currentArray.Length + items.Length];
                currentArray.CopyTo(newArray, 0);
                Array.Copy(items, 0, newArray, currentArray.Length, items.Length);

                _shardedList = new ShardedList<T>(newArray);
            }
        }

        public void Clear()
        {
            ClearAndGet();
        }

        public ShardedList<T> ClearAndGet()
        {
            lock (_writeLock)
            {
                var items = _shardedList;
                _shardedList = new ShardedList<T>();
                return items;
            }
        }

        public bool Contains(T item)
        {
            var currentArray = _shardedList.GetAllItems();
            return Array.IndexOf(currentArray, item) >= 0;
        }

        public void CopyTo(T[] array, int arrayIndex)
        {
            _shardedList.GetAllItems().CopyTo(array, arrayIndex);
        }

        public bool Remove(T item)
        {
            return RemoveAndCount(item).Item1;
        }

        public Tuple<bool, int> RemoveAndCount(T item)
        {
            lock (_writeLock)
            {
                var current = _shardedList;
                var currentArray = current.GetAllItems();
                var idx = Array.IndexOf(currentArray, item);
                if (idx < 0)
                {
                    return Tuple.Create(false, currentArray.Length);
                }

                if (currentArray.Length == 1 && idx == 0)
                {
                    _shardedList = new ShardedList<T>();
                    return Tuple.Create(true, 0);
                }

                var newArray = new T[currentArray.Length - 1];
                if (idx > 0)
                    Array.Copy(currentArray, 0, newArray, 0, idx);
                if (idx < currentArray.Length - 1)
                    Array.Copy(currentArray, idx + 1, newArray, idx, currentArray.Length - idx - 1);

                _shardedList = new ShardedList<T>(newArray);
                return Tuple.Create(true, newArray.Length);
            }
        }

        public ShardedList<T> GetSnapshot()
        {
            return _shardedList;
        }

        public T[] GetItemsForShard(int shardId)
        {
            return _shardedList.GetItemsForShard(shardId);
        }
    }
}
