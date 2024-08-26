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
    public sealed class CqlVector<T> : IInternalCqlVector
    {
        private readonly T[] _array;

        public CqlVector(T[] array)
        {
            _array = array ?? throw new ArgumentNullException(nameof(array));
        }

        public T this[int index]
        {
            get => _array[index];
            set => _array[index] = value;
        }

        object IInternalCqlVector.this[int index]
        {
            get => this[index];
            set => this[index] = (T)value;
        }

        public int Count => _array.Length;

        public Type GetSubType()
        {
            return typeof(T);
        }

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

        public bool Equals(CqlVector<T> other)
        {
            return CqlVector<T>.Equals(this, other);
        }

        public override bool Equals(object obj)
        {
            return ReferenceEquals(this, obj) || obj is CqlVector<T> other && Equals(other);
        }

        public override int GetHashCode()
        {
            return _array.GetHashCode();
        }

        public static bool operator ==(CqlVector<T> left, CqlVector<T> right)
        {
            return Equals(left, right);
        }

        public static bool operator !=(CqlVector<T> left, CqlVector<T> right)
        {
            return !Equals(left, right);
        }

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

            if (ReferenceEquals(one, other))
            {
                return true;
            }

            return Equals(one.AsArray(), other.AsArray());
        }
    }

    internal interface IInternalCqlVector : IEnumerable
    {
        object this[int index] { get; set; }

        int Count { get; }

        Type GetSubType();
    }
}
