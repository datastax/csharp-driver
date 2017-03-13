//
//  Copyright (C) 2017 DataStax, Inc.
//
//  Please see the license for details:
//  http://www.datastax.com/terms/datastax-dse-driver-license-terms
//

using System;
using System.Collections.Generic;
using System.Linq;
using Dse.Serialization.Search;
using NUnit.Framework;

namespace Dse.Test.Unit.Search
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
                var buffer = JoinBuffers(new[] {new byte[10], serialized}, serialized.Length + 10);
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
