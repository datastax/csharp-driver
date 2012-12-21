using System.Collections;
using System.Collections.Specialized;
using System;
using System.Collections.Generic;

namespace Cassandra.Native
{

    internal class DictSet<T> : IEnumerable<T>
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
    }
}
