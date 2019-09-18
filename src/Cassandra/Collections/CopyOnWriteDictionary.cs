using System;
using System.Collections;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace Cassandra.Collections
{
    /// <summary>
    /// A thread-safe variant of Dictionary{TKey, TValue} in which all mutative operations (Add and Remove) are implemented by making a copy of the underlying dictionary,
    /// intended to provide safe enumeration of its items.
    /// </summary>
    internal class CopyOnWriteDictionary<TKey, TValue> : IThreadSafeDictionary<TKey, TValue>, IReadOnlyDictionary<TKey, TValue>
    {
        private static readonly Dictionary<TKey, TValue> Empty = new Dictionary<TKey, TValue>();
        private volatile Dictionary<TKey, TValue> _map;
        private readonly object _writeLock = new object();

        public int Count => _map.Count;

        public bool IsReadOnly => false;

        public ICollection<TKey> Keys => _map.Keys;

        IEnumerable<TValue> IReadOnlyDictionary<TKey, TValue>.Values => Values;

        IEnumerable<TKey> IReadOnlyDictionary<TKey, TValue>.Keys => Keys;

        public ICollection<TValue> Values => _map.Values;

        public CopyOnWriteDictionary()
        {
            //Start with an instance without nodes
            _map = Empty;
        }

        public IEnumerator<KeyValuePair<TKey, TValue>> GetEnumerator()
        {
            return _map.GetEnumerator();
        }

        IEnumerator IEnumerable.GetEnumerator()
        {
            return GetEnumerator();
        }

        public void Clear()
        {
            _map = Empty;
        }

        public bool Contains(KeyValuePair<TKey, TValue> item)
        {
            return _map.Contains(item);
        }

        public void CopyTo(KeyValuePair<TKey, TValue>[] array, int arrayIndex)
        {
            _map.ToArray().CopyTo(array, arrayIndex);
        }

        public bool ContainsKey(TKey key)
        {
            return _map.ContainsKey(key);
        }

        /// <summary>
        /// Adds a key/value pair to the underlying dictionary if the key does not already exist.
        /// </summary>
        /// <param name="key">The key of the element to add.</param>
        /// <param name="value">the value to be added, if the key does not already exist</param>
        /// <returns>
        /// The value for the key. This will be either the existing value for the key if the 
        /// key is already in the dictionary, or the new value if the key was not in the dictionary.
        /// </returns>
        public TValue GetOrAdd(TKey key, TValue value)
        {
            lock (_writeLock)
            {
                TValue existingValue;
                if (_map.TryGetValue(key, out existingValue))
                {
                    return existingValue;
                }
                var newMap = new Dictionary<TKey, TValue>(_map);
                newMap.Add(key, value);
                _map = newMap;
                return value;
            }
        }

        /// <summary>
        /// Adds a new item by copying the underlying dictionary.
        /// </summary>
        /// <remarks>
        /// Adds or modifies an item.
        /// </remarks>
        public void Add(TKey key, TValue value)
        {
            lock (_writeLock)
            {
                var newMap = new Dictionary<TKey, TValue>(_map);
                newMap[key] = value;
                _map = newMap;
            }
        }

        /// <summary>
        /// Adds a new item by copying the underlying dictionary.
        /// </summary>
        /// <remarks>
        /// Adds or modifies an item.
        /// </remarks>
        public void Add(KeyValuePair<TKey, TValue> item)
        {
            Add(item.Key, item.Value);
        }

        /// <summary>
        /// Removes an item with the specified key by copying the underlying dictionary
        /// </summary>
        public bool Remove(TKey key)
        {
            lock (_writeLock)
            {
                if (!_map.ContainsKey(key))
                {
                    //Do not modify the underlying map
                    return false;
                }
                var newMap = new Dictionary<TKey, TValue>(_map);
                _map = newMap;
                return newMap.Remove(key);
            }
        }

        /// <summary>
        /// Removes an item by copying the underlying dictionary
        /// </summary>
        public bool Remove(KeyValuePair<TKey, TValue> item)
        {
            return Remove(item.Key);
        }

        /// <summary>
        /// Attempts to remove and return the the value with the specified key from the dictionary.
        /// </summary>
        public bool TryRemove(TKey key, out TValue value)
        {
            lock (_writeLock)
            {
                if (!_map.TryGetValue(key, out value))
                {
                    //Do not modify the underlying map
                    return false;
                }
                var newMap = new Dictionary<TKey, TValue>(_map);
                _map = newMap;
                newMap.Remove(key);
                return true;
            }
        }

        public TValue GetOrAdd(TKey key, Func<TKey, TValue> valueFactory)
        {
            lock (_writeLock)
            {
                TValue existingValue;
                if (_map.TryGetValue(key, out existingValue))
                {
                    return existingValue;
                }
                var newMap = new Dictionary<TKey, TValue>(_map);
                var value = valueFactory(key);
                newMap.Add(key, value);
                _map = newMap;
                return value;
            }
        }

        public bool TryGetValue(TKey key, out TValue value)
        {
            return _map.TryGetValue(key, out value);
        }

        public TValue this[TKey key]
        {
            get { return _map[key]; }
            set { Add(key, value); }
        }
    }
}
