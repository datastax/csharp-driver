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
    public class CqlToken
    {
        public readonly object[] Values;

        private CqlToken(object[] v)
        {
            Values = v;
        }

        public static CqlToken Create<T>(T v)
        {
            return new CqlToken(new object[] {v});
        }

        public static CqlToken Create<T1, T2>(T1 v1, T2 v2)
        {
            return new CqlToken(new object[] {v1, v2});
        }

        public static CqlToken Create<T1, T2, T3>(T1 v1, T2 v2, T3 v3)
        {
            return new CqlToken(new object[] {v1, v2, v3});
        }

        public static CqlToken Create<T1, T2, T3, T4>(T1 v1, T2 v2, T3 v3, T4 v4)
        {
            return new CqlToken(new object[] {v1, v2, v3, v4});
        }

        public static CqlToken Create<T1, T2, T3, T4, T5>(T1 v1, T2 v2, T3 v3, T4 v4, T5 v5)
        {
            return new CqlToken(new object[] {v1, v2, v3, v4, v5});
        }

        public static CqlToken Create<T1, T2, T3, T4, T5, T6>(T1 v1, T2 v2, T3 v3, T4 v4, T5 v5, T6 v6)
        {
            return new CqlToken(new object[] {v1, v2, v3, v4, v5, v6});
        }

        public override int GetHashCode()
        {
            throw new InvalidOperationException();
        }

        public static bool operator ==(CqlToken a, object b)
        {
            throw new InvalidOperationException();
        }

        public static bool operator !=(CqlToken a, object b)
        {
            throw new InvalidOperationException();
        }

        public static bool operator <=(CqlToken a, object b)
        {
            throw new InvalidOperationException();
        }

        public static bool operator >=(CqlToken a, object b)
        {
            throw new InvalidOperationException();
        }

        public static bool operator <(CqlToken a, object b)
        {
            throw new InvalidOperationException();
        }

        public static bool operator >(CqlToken a, object b)
        {
            throw new InvalidOperationException();
        }

        public static bool operator !=(CqlToken a, CqlToken b)
        {
            throw new InvalidOperationException();
        }

        public override bool Equals(object obj)
        {
            throw new InvalidOperationException();
        }

        public static bool operator ==(CqlToken a, CqlToken b)
        {
            throw new InvalidOperationException();
        }

        public static bool operator <=(CqlToken a, CqlToken b)
        {
            throw new InvalidOperationException();
        }

        public static bool operator >=(CqlToken a, CqlToken b)
        {
            throw new InvalidOperationException();
        }

        public static bool operator <(CqlToken a, CqlToken b)
        {
            throw new InvalidOperationException();
        }

        public static bool operator >(CqlToken a, CqlToken b)
        {
            throw new InvalidOperationException();
        }
    }
}