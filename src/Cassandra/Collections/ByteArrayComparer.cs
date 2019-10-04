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
using System.Collections.Generic;
using System.Linq;

namespace Cassandra.Collections
{
    internal class ByteArrayComparer : IEqualityComparer<byte[]>
    {
        public bool Equals(byte[] a, byte[] b)
        {
            if (a == null || b == null)
            {
                return a == b;
            }
            return a.SequenceEqual(b);
        }

        public int GetHashCode(byte[] key)
        {
            if (key == null)
            {
                throw new ArgumentNullException(nameof(key));
            }
            var hash = 0;
            var rest = key.Length % 4;
            for (var i = 0; i < key.Length - rest; i += 4)
            {
                // Use int32 values
                hash = Utils.CombineHashCode(
                    new[] { hash, BeConverter.ToInt32(new [] { key[i], key[i+1], key[i+2], key[i+3] }) });
            }
            if (rest > 0)
            {
                var arr = new byte[4];
                for (var i = 0; i < rest; i++)
                {
                    arr[i] = key[key.Length - rest + i];
                }
                hash = Utils.CombineHashCode(new[] { hash, BeConverter.ToInt32(arr) });
            }
            return hash;
        }
    }
}