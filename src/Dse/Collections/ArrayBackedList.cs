//
//  Copyright (C) DataStax, Inc.
//
//  Please see the license for details:
//  http://www.datastax.com/terms/datastax-dse-driver-license-terms
//

using System;
using System.Collections;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace Dse.Collections
{
    /// <summary>
    /// It creates a IList{T} wrapper of an array to avoid extra allocations of List{T} for read-only lists
    /// </summary>
    internal class ArrayBackedList<T> : IList<T>
    {
        private readonly T[] _items;

        public ArrayBackedList(T[] items)
        {
            _items = items;
        }

        public IEnumerator<T> GetEnumerator()
        {
            return ((IEnumerable<T>)_items).GetEnumerator();
        }

        IEnumerator IEnumerable.GetEnumerator()
        {
            return GetEnumerator();
        }

        public void Add(T item)
        {
            throw new NotSupportedException();
        }

        public void Clear()
        {
            throw new NotSupportedException();
        }

        public bool Contains(T item)
        {
            return _items.Contains(item);
        }

        public void CopyTo(T[] array, int arrayIndex)
        {
            _items.CopyTo(array, arrayIndex);
        }

        public bool Remove(T item)
        {
            throw new NotSupportedException();
        }

        public int Count
        {
            get { return _items.Length; }
        }

        public bool IsReadOnly {
            get { return true; }
        }

        public int IndexOf(T item)
        {
            return Array.IndexOf(_items, item);
        }

        public void Insert(int index, T item)
        {
            throw new NotSupportedException();
        }

        public void RemoveAt(int index)
        {
            throw new NotSupportedException();
        }

        public T this[int index]
        {
            get { return _items[index]; }
            set { throw new NotSupportedException(); }
        }
    }
}
