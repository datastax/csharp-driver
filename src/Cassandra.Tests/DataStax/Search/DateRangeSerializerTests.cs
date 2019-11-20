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
using Cassandra.Serialization.Search;
using NUnit.Framework;

namespace Cassandra.Tests.DataStax.Search
{
    public class DateRangeSerializerTests : BaseUnitTest
    {
        [Test]
        public void Serialize_Deserialize_Test()
        {
            var serializer = new DateRangeSerializer();
            foreach (var value in DateRangeTests.Values.Select(e => e.Item3))
            {
                var serialized = serializer.Serialize(4, value);
                // Use a buffer at a different index than 0
                var buffer = DateRangeSerializerTests.JoinBuffers(new[] {new byte[10], serialized}, serialized.Length + 10);
                var deserialized = serializer.Deserialize(4, buffer, 10, serialized.Length, serializer.TypeInfo);
                Assert.AreEqual(value, deserialized);
            }
        }

        private static byte[] JoinBuffers(IEnumerable<byte[]> buffers, int totalLength)
        {
            var result = new byte[totalLength];
            var offset = 0;
            foreach (byte[] data in buffers)
            {
                Buffer.BlockCopy(data, 0, result, offset, data.Length);
                offset += data.Length;
            }
            return result;
        }
    }
}
