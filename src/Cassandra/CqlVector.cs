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

namespace Cassandra
{
    public sealed class CqlVector<T> : IReadOnlyCollection<T>, IInternalCqlVector
    {
        private T[] _array;

        internal CqlVector()
        {
        }

        public CqlVector(int dimension)
        {
            if (dimension <= 0) // C* only allows positive dimension
            {
                throw new ArgumentOutOfRangeException(nameof(dimension), dimension, "Vector dimension can not be zero or negative.");
            }
            _array = new T[dimension];
        }

        public CqlVector(params T[] elements)
        {
            if (elements == null)
            {
                throw new ArgumentNullException(nameof(elements));
            }
            if (elements.Length <= 0) // C* only allows positive dimension
            {
                throw new ArgumentOutOfRangeException(nameof(elements), elements, "Vector dimension can not be zero or negative.");
            }
            _array = elements;
        }

        public static CqlVector<T> FromArray(T[] array)
        {
            var v = new CqlVector<T>
            {
                _array = array
            };
            return v;
        }

        public T this[int index]
        {
            get => _array[index];
            set => _array[index] = value;
        }

        public int Count => _array.Length;

        public T[] AsArray()
        {
            return _array;
        }

        public IEnumerator<T> GetEnumerator()
        {
            return _array.AsEnumerable().GetEnumerator();
        }

        IEnumerator IEnumerable.GetEnumerator()
        {
            return _array.GetEnumerator();
        }

        public override bool Equals(object obj)
        {
            return obj is CqlVector<T> other && Equals(other);
        }

        public override int GetHashCode()
        {
            return _array.GetHashCode();
        }

        public static bool operator ==(CqlVector<T> left, CqlVector<T> right)
        {
            return CqlVector<T>.Equals(left, right);
        }

        public static bool operator !=(CqlVector<T> left, CqlVector<T> right)
        {
            return !CqlVector<T>.Equals(left, right);
        }

        public override string ToString()
        {
            return _array.ToString();
        }

        public static explicit operator T[](CqlVector<T> v) => v.AsArray();

        public static implicit operator CqlVector<T>(T[] a) => new CqlVector<T>(a);

        private static bool Equals(CqlVector<T> one, CqlVector<T> other)
        {
            if (one is null && other is null)
            {
                return true;
            }
            if (one is null || other is null)
            {
                return false;
            }

            if (object.ReferenceEquals(one, other))
            {
                return true;
            }

            return one._array.SequenceEqual(other._array);
        }

        object IInternalCqlVector.this[int index]
        {
            get => this[index];
            set => this[index] = (T)value;
        }

        Type IInternalCqlVector.GetSubType()
        {
            return typeof(T);
        }

        void IInternalCqlVector.SetArray(object array)
        {
            _array = (T[])array;
        }
    }

    internal interface IInternalCqlVector : IEnumerable
    {
        object this[int index] { get; set; }

        int Count { get; }

        Type GetSubType();

        void SetArray(object array);
    }
}
