//
//  Copyright (C) DataStax, Inc.
//
//  Please see the license for details:
//  http://www.datastax.com/terms/datastax-dse-driver-license-terms
//

using System;
using System.IO;
using Dse.Responses;
using Dse.Serialization;
using NUnit.Framework;

namespace Dse.Test.Unit
{
    [TestFixture]
    public class AbstractResponseTest
    {
        [Test]
        public void Ctor_Null_Throws()
        {
            // ReSharper disable once ObjectCreationAsStatement
            Assert.Throws<ArgumentNullException>(() => new Response(null));
        }

        [Test]
        public void Ctor_NoFlags_TraceIdIsNull()
        {
            // Arrange
            var frame = new Frame(new FrameHeader(), new MemoryStream(), new SerializerManager(ProtocolVersion.V4).GetCurrentSerializer());

            // Act
            var uut = new Response(frame);

            // Assert
            Assert.IsNull(uut.TraceId);
        }

        [Test]
        public void Ctor_NoFlags_BodyStreamPositionIsZero()
        {
            // Arrange
            var frame = new Frame(new FrameHeader(), new MemoryStream(new byte[] { 1 }), new SerializerManager(ProtocolVersion.V4).GetCurrentSerializer());

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
            var expected = new Guid(TypeSerializer.GuidShuffle(buffer));
            var body = new MemoryStream(buffer);
            var frame = new Frame(header, body, new SerializerManager(ProtocolVersion.V4).GetCurrentSerializer());

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
            var frame = new Frame(header, body, new SerializerManager(ProtocolVersion.V4).GetCurrentSerializer());

            // Act
            new Response(frame);

            // Assert
            Assert.AreEqual(16, body.Position);
            Assert.AreEqual(20, body.Length);
        }
    }
}