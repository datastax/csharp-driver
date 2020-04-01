//
//  Copyright (C) DataStax, Inc.
//
//  Please see the license for details:
//  http://www.datastax.com/terms/datastax-dse-driver-license-terms
//

using System;

namespace Dse.Data.Linq
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
