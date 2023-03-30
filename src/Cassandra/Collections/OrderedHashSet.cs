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

using System.Collections;
using System.Collections.Generic;
using Cassandra.MetadataHelpers;

namespace Cassandra.Collections
{
    /// <summary>
    /// Ordered hash set designed for building token to replica maps (specifically for keyspaces configured with <see cref="NetworkTopologyStrategy"/>)
    /// <para></para>
    /// See <see cref="NetworkTopologyStrategy.ComputeTokenToReplicaMap"/>.
    /// </summary>
    internal class OrderedHashSet<T> : ISet<T>
    {
        private readonly HashSet<T> _set;
        private readonly LinkedList<T> _list;

        public int Count => _set.Count;

        public bool IsReadOnly => false;

        public OrderedHashSet()
        {
            _set = new HashSet<T>();
            _list = new LinkedList<T>();
        }

        public IEnumerator<T> GetEnumerator()
        {
            return _list.GetEnumerator();
        }

        IEnumerator IEnumerable.GetEnumerator()
        {
            return GetEnumerator();
        }

        void ICollection<T>.Add(T item)
        {
            Add(item);
        }

        public void UnionWith(IEnumerable<T> other)
        {
            _set.UnionWith(other);
        }

        public void IntersectWith(IEnumerable<T> other)
        {
            _set.IntersectWith(other);
        }

        public void ExceptWith(IEnumerable<T> other)
        {
            _set.ExceptWith(other);
        }

        public void SymmetricExceptWith(IEnumerable<T> other)
        {
            _set.SymmetricExceptWith(other);
        }

        public bool IsSubsetOf(IEnumerable<T> other)
        {
            return _set.IsSubsetOf(other);
        }

        public bool IsSupersetOf(IEnumerable<T> other)
        {
            return _set.IsSupersetOf(other);
        }

        public bool IsProperSupersetOf(IEnumerable<T> other)
        {
            return _set.IsProperSupersetOf(other);
        }

        public bool IsProperSubsetOf(IEnumerable<T> other)
        {
            return _set.IsProperSubsetOf(other);
        }

        public bool Overlaps(IEnumerable<T> other)
        {
            return _set.Overlaps(other);
        }

        public bool SetEquals(IEnumerable<T> other)
        {
            return _set.SetEquals(other);
        }

        public bool Add(T item)
        {
            var added = _set.Add(item);
            if (added)
            {
                _list.AddLast(item);
            }
            return added;
        }

        public void Clear()
        {
            _set.Clear();
            _list.Clear();
        }

        public bool Contains(T item)
        {
            return _set.Contains(item);
        }

        public void CopyTo(T[] array, int arrayIndex)
        {
            _set.CopyTo(array, arrayIndex);
        }

        public bool Remove(T item)
        {
            var removed = _set.Remove(item);
            if (removed)
            {
                _list.Remove(item);
            }
            return removed;
        }
    }
}