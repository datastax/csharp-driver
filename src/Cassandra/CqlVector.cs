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
    /// <summary>
    /// <para>Type used by the driver to represent the Apache Cassandra Vector type.</para>
    /// <para>
    /// Examples of how to create a vector object (using int as a sub type here but all CQL types are supported):
    /// <code>
    /// var vector = new CqlVector&lt;int&gt;(1, 2, 3);
    /// 
    /// var vector = CqlVector&lt;int&gt;.New(new int[] { 1, 2, 3 });
    ///
    /// // note that ToArray() performs a copy 
    /// var vector = CqlVector&lt;int&gt;.New(new List&lt;int&gt; { 1, 2, 3 }.ToArray());
    /// 
    /// var vector = CqlVector&lt;int&gt;.New(3);
    /// vector[0] = 1;
    /// vector[1] = 2;
    /// vector[2] = 3;
    ///
    /// // no copy
    /// var vector = new int[] { 1, 2, 3 }.AsCqlVector();
    ///
    /// // with copy
    /// var vector = new int[] { 1, 2, 3 }.ToCqlVector();
    /// </code>
    /// </para>
    /// </summary>
    /// <typeparam name="T">Type of the vector elements.</typeparam>
    public sealed class CqlVector<T> : IReadOnlyCollection<T>, IInternalCqlVector
    {
        private static readonly T[] Empty = new T[0];

        private T[] _array;

        /// <summary>
        /// Creates a new vector with an empty array.
        /// </summary>
        public CqlVector()
        {
            _array = Empty;
        }

        /// <summary>
        /// Creates a new vector with the provided array. Note that no copy is made, the provided array is used directly by the new vector object.
        /// </summary>
        public CqlVector(params T[] elements)
        {
            _array = elements ?? throw new ArgumentNullException(nameof(elements));
        }

        /// <summary>
        /// Creates a new vector with the provided number of dimensions.
        /// </summary>
        public static CqlVector<T> New(int dimensions)
        {
            if (dimensions == 0)
            {
                return new CqlVector<T>();
            }

            if (dimensions < 0)
            {
                throw new ArgumentOutOfRangeException(nameof(dimensions), dimensions, "Vector dimensions can not be negative.");
            }

            return new CqlVector<T>(new T[dimensions]);
        }

        /// <summary>
        /// Creates a new vector with the provided array.
        /// This is equivalent to calling the constructor <see cref="CqlVector{T}(T[])"/>. Note that no copy is made, the provided array is used directly by the new vector object.
        /// </summary>
        public static CqlVector<T> New(T[] arr)
        {
            return new CqlVector<T>(arr);
        }

        /// <summary>Gets or sets the element at the specified index.</summary>
        /// <param name="index">The zero-based index of the element to get or set.</param>
        /// <returns>The element at the specified index.</returns>
        /// <exception cref="T:System.ArgumentOutOfRangeException"><paramref name="index">index</paramref> is less than 0.   -or-  <paramref name="index">index</paramref> is equal to or greater than <see cref="CqlVector{T}.Count"></see>.</exception>

        public T this[int index]
        {
            get => _array[index];
            set => _array[index] = value;
        }

        /// <summary>
        /// Gets the size of this vector (number of dimensions).
        /// </summary>
        public int Count => _array.Length;

        /// <summary>
        /// Gets the underlying array. No copy is made. If a copy is desired, use the IEnumerable extension method <see cref="System.Linq.Enumerable.ToArray{T}(IEnumerable{T})"/>.
        /// </summary>
        /// <returns>The underlying array.</returns>
        public T[] AsArray()
        {
            return _array;
        }

        /// <inheritdoc/>
        public IEnumerator<T> GetEnumerator()
        {
            return _array.AsEnumerable().GetEnumerator();
        }

        /// <inheritdoc/>
        IEnumerator IEnumerable.GetEnumerator()
        {
            return _array.GetEnumerator();
        }

        /// <inheritdoc/>
        public override bool Equals(object obj)
        {
            return obj is CqlVector<T> other && CqlVector<T>.Equals(this, other);
        }

        /// <inheritdoc/>
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

        Array IInternalCqlVector.GetArray()
        {
            return _array;
        }
    }

    internal interface IInternalCqlVector : IEnumerable
    {
        object this[int index] { get; set; }

        int Count { get; }

        Type GetSubType();

        void SetArray(object array);

        Array GetArray();
    }
}
