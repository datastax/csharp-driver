//
//      Copyright (C) 2012 DataStax Inc.
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
using System.Collections.Specialized;
using System;
using System.Collections.Generic;

namespace Cassandra
{
#if NET_40_OR_GREATER
    internal class DictSet<T> : HashSet<T>
    {
        public DictSet() { }
        public DictSet(IEnumerable<T> c) : base(c) {}
        public bool AddRange(IEnumerable<T> c)
        {
            bool changed = false;
            foreach (var o in c)
                changed |= this.Add(o);
            return changed;
        }
        public bool IsEmpty
        {
            get { return Count == 0; }
        }
    }
#else
    internal class DictSet<T> : IEnumerable<T>, ICollection<T>
    {
        public DictSet() { }

        public DictSet(IEnumerable<T> c) { this.AddRange(c); }

        protected IDictionary<T, object> InternalDictionary = new Dictionary<T, object>();

        public bool Add(T o)
        {
            if (InternalDictionary.ContainsKey(o))
                return false;
            else
            {
                InternalDictionary.Add(o, null);
                return true;
            }
        }

        public bool AddRange(IEnumerable<T> c)
        {
            bool changed = false;
            foreach (var o in c)
                changed |= this.Add(o);
            return changed;
        }

        public void Clear()
        {
            InternalDictionary.Clear();
        }

        public bool Contains(T o)
        {
            return InternalDictionary.ContainsKey(o);
        }

        public bool IsEmpty
        {
            get { return InternalDictionary.Count == 0; }
        }

        public bool Remove(T o)
        {
            bool contained = this.Contains(o);
            if (contained)
                InternalDictionary.Remove(o);
            return contained;
        }

        public bool RemoveRange(IEnumerable<T> c)
        {
            bool changed = false;
            foreach (var o in c)
                changed |= this.Remove(o);
            return changed;
        }

        public int Count
        {
            get { return InternalDictionary.Count; }
        }


        public IEnumerator<T> GetEnumerator()
        {
            return InternalDictionary.Keys.GetEnumerator();
        }

        IEnumerator IEnumerable.GetEnumerator()
        {
            return InternalDictionary.Keys.GetEnumerator();
        }

        void ICollection<T>.Add(T item)
        {
            Add(item);
        }

        public void CopyTo(T[] array, int arrayIndex)
        {
            int i = arrayIndex;
            foreach (var item in this)
                array[i++] = item;
        }

        public bool IsReadOnly
        {
            get { throw new NotImplementedException(); }
        }
    }
#endif
}
