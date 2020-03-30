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

namespace Cassandra.Data.Linq
{
    /// <summary>
    /// Represents a set of functions that are defined at CQL level
    /// </summary>
    public sealed class CqlFunction
    {
        internal object[] Values { get; private set; }

        /// <summary>
        /// Only suitable for Linq Expression generations
        /// </summary>
        private CqlFunction() { }

        /// <summary>
        /// CQL function maxTimeuuid() that returns biggest timeuuid value having the provided timestamp
        /// </summary>
        public static CqlFunction MaxTimeUuid(DateTimeOffset value)
        {
            return null;
        }

        /// <summary>
        /// CQL function maxTimeuuid() that returns smallest timeuuid value having the provided timestamp
        /// </summary>
        public static CqlFunction MinTimeUuid(DateTimeOffset value)
        {
            return null;
        }

        /// <summary>
        /// CQL function token
        /// </summary>
        public static CqlFunction Token(object key)
        {
            return null;
        }

        /// <summary>
        /// CQL function token
        /// </summary>
        public static CqlFunction Token(object key1, object key2)
        {
            return null;
        }

        /// <summary>
        /// CQL function token
        /// </summary>
        public static CqlFunction Token(object key1, object key2, object key3)
        {
            return null;
        }

        /// <summary>
        /// CQL function token
        /// </summary>
        public static CqlFunction Token(object key1, object key2, object key3, object key4)
        {
            return null;
        }

        public static bool operator ==(CqlFunction a, object b)
        {
            throw new InvalidOperationException();
        }

        public static bool operator ==(object b, CqlFunction a)
        {
            throw new InvalidOperationException();
        }

        public static bool operator ==(CqlFunction b, CqlFunction a)
        {
            throw new InvalidOperationException();
        }

        public static bool operator !=(CqlFunction a, object b)
        {
            throw new InvalidOperationException();
        }

        public static bool operator !=(object b, CqlFunction a)
        {
            throw new InvalidOperationException();
        }

        public static bool operator !=(CqlFunction b, CqlFunction a)
        {
            throw new InvalidOperationException();
        }

        public static bool operator <=(CqlFunction a, object b)
        {
            throw new InvalidOperationException();
        }

        public static bool operator <=(object b, CqlFunction a)
        {
            throw new InvalidOperationException();
        }

        public static bool operator <=(CqlFunction b, CqlFunction a)
        {
            throw new InvalidOperationException();
        }

        public static bool operator >=(CqlFunction a, object b)
        {
            throw new InvalidOperationException();
        }

        public static bool operator >=(object b, CqlFunction a)
        {
            throw new InvalidOperationException();
        }

        public static bool operator >=(CqlFunction b, CqlFunction a)
        {
            throw new InvalidOperationException();
        }

        public static bool operator <(CqlFunction a, object b)
        {
            throw new InvalidOperationException();
        }

        public static bool operator <(object a, CqlFunction b)
        {
            throw new InvalidOperationException();
        }

        public static bool operator <(CqlFunction a, CqlFunction b)
        {
            throw new InvalidOperationException();
        }

        public static bool operator >(CqlFunction a, object b)
        {
            throw new InvalidOperationException();
        }

        public static bool operator >(object b, CqlFunction a)
        {
            throw new InvalidOperationException();
        }

        public static bool operator >(CqlFunction a, CqlFunction b)
        {
            throw new InvalidOperationException();
        }

        public override bool Equals(object obj)
        {
            //Not suitable for equality comparisons
            throw new InvalidOperationException();
        }

        public override int GetHashCode()
        {
            //Not suitable for hash trees
            throw new InvalidOperationException();
        }

        public static object ToObject(CqlFunction value)
        {
            return (object) value;
        }

        public static implicit operator Guid(CqlFunction value)
        {
            return Guid.Empty;
        }
    }
}
