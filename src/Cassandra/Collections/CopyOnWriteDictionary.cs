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
using System.Linq;

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

        public CopyOnWriteDictionary(IDictionary<TKey, TValue> toCopy)
        {
            _map = new Dictionary<TKey, TValue>(toCopy);
        }
        
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
            if (key == null)
            {
                throw new ArgumentNullException(nameof(key));
            }

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
            if (key == null)
            {
                throw new ArgumentNullException(nameof(key));
            }

            // optimistic scenario: return before lock
            if (_map.TryGetValue(key, out var outPutValue))
            {
                return outPutValue;
            }

            lock (_writeLock)
            {
                if (_map.TryGetValue(key, out TValue existingValue))
                {
                    return existingValue;
                }

                CloneMapAndAddUnsafe(key, value);
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
            if (key == null)
            {
                throw new ArgumentNullException(nameof(key));
            }

            lock (_writeLock)
            {
                CloneMapAndAddUnsafe(key, value);
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
            if (key == null)
            {
                throw new ArgumentNullException(nameof(key));
            }

            lock (_writeLock)
            {
                if (!_map.ContainsKey(key))
                {
                    //Do not modify the underlying map
                    return false;
                }

                return CloneMapAndRemoveUnsafe(key);
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
            if (key == null)
            {
                throw new ArgumentNullException(nameof(key));
            }

            lock (_writeLock)
            {
                if (!_map.TryGetValue(key, out value))
                {
                    //Do not modify the underlying map
                    return false;
                }

                return CloneMapAndRemoveUnsafe(key);
            }
        }

        public TValue GetOrAdd(TKey key, Func<TKey, TValue> valueFactory)
        {
            if (key == null)
            {
                throw new ArgumentNullException(nameof(key));
            }

            if (valueFactory == null)
            {
                throw new ArgumentNullException(nameof(valueFactory));
            }

            // optimistic scenario: return before lock
            if (_map.TryGetValue(key, out var outputValue))
            {
                return outputValue;
            }

            lock (_writeLock)
            {
                if (_map.TryGetValue(key, out TValue existingValue))
                {
                    return existingValue;
                }
                var value = valueFactory(key);
                CloneMapAndAddUnsafe(key, value);
                return value;
            }
        }

        public TValue AddOrUpdate(
            TKey key, Func<TKey, TValue> addValueFactory, Func<TKey, TValue, TValue> updateValueFactory)
        {
            if (key == null)
            {
                throw new ArgumentNullException(nameof(key));
            }

            if (addValueFactory == null)
            {
                throw new ArgumentNullException(nameof(addValueFactory));
            }

            if (updateValueFactory == null)
            {
                throw new ArgumentNullException(nameof(updateValueFactory));
            }

            lock (_writeLock)
            {
                TValue newValue;
                if (TryGetValue(key, out var existingValue))
                {
                    newValue = updateValueFactory(key, existingValue);
                    CloneMapAndUpdateUnsafe(key, newValue);
                    return newValue;
                }

                newValue = addValueFactory(key);
                CloneMapAndAddUnsafe(key, newValue);
                return newValue;
            }
        }

        /// <inheritdoc />
        public TValue CompareAndUpdate(
            TKey key, Func<TKey, TValue, bool> compareFunc, Func<TKey, TValue, TValue> updateValueFactory)
        {
            if (key == null)
            {
                throw new ArgumentNullException(nameof(key));
            }
            
            if (updateValueFactory == null)
            {
                throw new ArgumentNullException(nameof(updateValueFactory));
            }

            lock (_writeLock)
            {
                if (!TryGetValue(key, out var existingValue))
                {
                    throw new InvalidOperationException("Could not retrieve an item with that key.");
                }

                if (!compareFunc(key, existingValue))
                {
                    return existingValue;
                }

                var newValue = updateValueFactory(key, existingValue);
                CloneMapAndUpdateUnsafe(key, newValue);
                return newValue;
            }
        }

        private void CloneMapAndAddUnsafe(TKey key, TValue value)
        {
            var newMap = new Dictionary<TKey, TValue>(_map)
            {
                { key, value }
            };

            _map = newMap;
        }

        private void CloneMapAndUpdateUnsafe(TKey key, TValue value)
        {
            var newMap = new Dictionary<TKey, TValue>(_map)
            {
                [key] = value
            };

            _map = newMap;
        }
        
        private bool CloneMapAndRemoveUnsafe(TKey key)
        {
            var newMap = new Dictionary<TKey, TValue>(_map);
            var success = newMap.Remove(key);
            _map = newMap;
            return success;
        }

        public bool TryGetValue(TKey key, out TValue value)
        {
            if (key == null)
            {
                throw new ArgumentNullException(nameof(key));
            }

            return _map.TryGetValue(key, out value);
        }

        public TValue this[TKey key]
        {
            get => _map[key];
            set => Add(key, value);
        }
    }
}
