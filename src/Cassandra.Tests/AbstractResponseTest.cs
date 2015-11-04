//
//      Copyright (C) 2012 DataStax Inc.
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
using System.IO;
using Cassandra.Responses;
using NUnit.Framework;

namespace Cassandra.Tests
{
    [TestFixture]
    public class AbstractResponseTest
    {
        [Test]
        [ExpectedException(typeof(ArgumentNullException))]
        public void Ctor_Null_Throws()
        {
            new Response(null);
        }

        [Test]
        public void Ctor_NoFlags_TraceIdIsNull()
        {
            // Arrange
            var frame = new Frame(new FrameHeader(), new MemoryStream());

            // Act
            var uut = new Response(frame);

            // Assert
            Assert.IsNull(uut.TraceId);
        }

        [Test]
        public void Ctor_NoFlags_BodyStreamPositionIsZero()
        {
            // Arrange
            var frame = new Frame(new FrameHeader(), new MemoryStream(new byte[] { 1 }));

            // Act
            new Response(frame);

            // Assert
            Assert.AreEqual(0, frame.Body.Position);
        }

        [Test]
        public void Ctor_TraceFlagSet_TraceIdIsSet()
        {
            // Arrange
            var header = new FrameHeader {Flags = FrameHeader.HeaderFlag.Tracing};
            var rnd = new Random();
            var buffer = new byte[16];
            rnd.NextBytes(buffer);
            var expected = new Guid(TypeCodec.GuidShuffle(buffer));
            var body = new MemoryStream(buffer);
            var frame = new Frame(header, body);

            // Act
            var uut = new Response(frame);

            // Assert
            Assert.AreEqual(expected, uut.TraceId);
        }

        [Test]
        public void Ctor_TraceFlagSet_BytesReadFromFrame()
        {
            // Arrange
            var header = new FrameHeader { Flags = FrameHeader.HeaderFlag.Tracing };
            var body = new MemoryStream(new byte[20]);
            var frame = new Frame(header, body);

            // Act
            new Response(frame);

            // Assert
            Assert.AreEqual(16, body.Position);
            Assert.AreEqual(20, body.Length);
        }
    }
}