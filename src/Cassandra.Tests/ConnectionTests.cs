using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Net;
using System.Text;
using System.Threading;
using Cassandra.Responses;
using Cassandra.Serialization;
using Microsoft.IO;
using Moq;
using NUnit.Framework;

namespace Cassandra.Tests
{
    [TestFixture]
    public class ConnectionTests
    {
        private static readonly IPEndPoint Address = new IPEndPoint(IPAddress.Parse("127.0.0.1"), 1000);
        private const int TestFrameLength = 12;

        private static Mock<Connection> GetConnectionMock(Configuration config = null)
        {
            config = config ?? new Configuration();
            return new Mock<Connection>(
                MockBehavior.Loose, new Serializer(ProtocolVersion.MaxSupported), Address, config);
        }

        [Test]
        public void ReadParse_Handles_Complete_Frames_In_Different_Buffers()
        {
            var connectionMock = GetConnectionMock();
            var streamIds = new List<short>();
            var responses = new ConcurrentBag<Response>();
            connectionMock.Setup(c => c.RemoveFromPending(It.IsAny<short>()))
                .Callback<short>(id => streamIds.Add(id))
                .Returns(() => new OperationState((ex, r) => responses.Add(r)));
            var connection = connectionMock.Object;
            var buffer = GetResultBuffer(127);
            connection.ReadParse(buffer, buffer.Length);
            CollectionAssert.AreEqual(new short[] { 127 }, streamIds);
            buffer = GetResultBuffer(126);
            connection.ReadParse(buffer, buffer.Length);
            CollectionAssert.AreEqual(new short[] { 127, 126 }, streamIds);
            buffer = GetResultBuffer(125);
            connection.ReadParse(buffer, buffer.Length);
            CollectionAssert.AreEqual(new short[] { 127, 126, 125 }, streamIds);
            TestHelper.WaitUntil(() => responses.Count == 3);
            Assert.AreEqual(3, responses.Count);
            CollectionAssert.AreEqual(Enumerable.Repeat(ResultResponse.ResultResponseKind.Void, 3), responses.Select(r => ((ResultResponse) r).Kind));
        }

        [Test]
        public void ReadParse_Handles_Complete_Frames_In_A_Single_Frame()
        {
            var connectionMock = GetConnectionMock();
            var streamIds = new List<short>();
            var responses = new ConcurrentBag<Response>();
            connectionMock.Setup(c => c.RemoveFromPending(It.IsAny<short>()))
                .Callback<short>(id => streamIds.Add(id))
                .Returns(() => new OperationState((ex, r) => responses.Add(r)));
            var connection = connectionMock.Object;
            var buffer = GetResultBuffer(127).Concat(GetResultBuffer(126)).ToArray();
            connection.ReadParse(buffer, buffer.Length);
            CollectionAssert.AreEqual(new short[] { 127, 126 }, streamIds);
            TestHelper.WaitUntil(() => responses.Count == 2);
            Assert.AreEqual(2, responses.Count);
        }

        [Test]
        public void ReadParse_Handles_UnComplete_Header()
        {
            var connectionMock = GetConnectionMock();
            var streamIds = new List<short>();
            var responses = new ConcurrentBag<Response>();
            connectionMock.Setup(c => c.RemoveFromPending(It.IsAny<short>()))
                .Callback<short>(id => streamIds.Add(id))
                .Returns(() => new OperationState((ex, r) => responses.Add(r)));
            var connection = connectionMock.Object;
            var buffer = GetResultBuffer(127).Concat(GetResultBuffer(126)).Concat(GetResultBuffer(100)).ToArray();
            //first 2 messages and 2 bytes of the third message
            var firstSlice = buffer.Length - TestFrameLength + 2;
            connection.ReadParse(buffer, firstSlice);
            CollectionAssert.AreEqual(new short[] { 127, 126 }, streamIds);
            buffer = buffer.Skip(firstSlice).ToArray();
            connection.ReadParse(buffer, buffer.Length);
            CollectionAssert.AreEqual(new short[] { 127, 126, 100 }, streamIds);
            TestHelper.WaitUntil(() => responses.Count == 3);
            CollectionAssert.AreEqual(Enumerable.Repeat(ResultResponse.ResultResponseKind.Void, 3), responses.Select(r => ((ResultResponse)r).Kind));
            buffer = GetResultBuffer(99);
            connection.ReadParse(buffer, buffer.Length);
            CollectionAssert.AreEqual(new short[] { 127, 126, 100, 99 }, streamIds);
        }

        [Test]
        public void ReadParse_Handles_UnComplete_Header_In_Multiple_Messages()
        {
            var connectionMock = GetConnectionMock();
            var streamIds = new List<short>();
            var responses = new ConcurrentBag<Response>();
            connectionMock.Setup(c => c.RemoveFromPending(It.IsAny<short>()))
                .Callback<short>(id => streamIds.Add(id))
                .Returns(() => new OperationState((ex, r) => responses.Add(r)));
            var connection = connectionMock.Object;
            var buffer = GetResultBuffer(127).Concat(GetResultBuffer(126)).Concat(GetResultBuffer(100)).ToArray();
            //first 2 messages and 2 bytes of the third message
            var length = buffer.Length - TestFrameLength + 2;
            connection.ReadParse(buffer, length);
            CollectionAssert.AreEqual(new short[] { 127, 126 }, streamIds);
            buffer = buffer.Skip(length).ToArray();
            length = buffer.Length - 8;
            //header is still not completed
            connection.ReadParse(buffer, length);
            CollectionAssert.AreEqual(new short[] { 127, 126 }, streamIds);
            //header and body are completed
            buffer = buffer.Skip(length).ToArray();
            length = buffer.Length;
            connection.ReadParse(buffer, length);
            CollectionAssert.AreEqual(new short[] { 127, 126, 100 }, streamIds);
            TestHelper.WaitUntil(() => responses.Count == 3);
            CollectionAssert.AreEqual(Enumerable.Repeat(ResultResponse.ResultResponseKind.Void, 3), responses.Select(r => ((ResultResponse)r).Kind));
            buffer = GetResultBuffer(99);
            connection.ReadParse(buffer, buffer.Length);
            CollectionAssert.AreEqual(new short[] { 127, 126, 100, 99 }, streamIds);
        }

        [Test]
        public void ReadParse_Handles_UnComplete_Body()
        {
            var connectionMock = GetConnectionMock();
            var streamIds = new List<short>();
            var responses = new ConcurrentBag<Response>();
            var exceptions = new ConcurrentBag<Exception>();
            connectionMock.Setup(c => c.RemoveFromPending(It.IsAny<short>()))
                .Callback<short>(id => streamIds.Add(id))
                .Returns(() => new OperationState((ex, r) =>
                {
                    if (ex != null)
                    {
                        exceptions.Add(ex);
                        return;
                    }
                    responses.Add(r);
                }));
            var connection = connectionMock.Object;
            var buffer = GetResultBuffer(127).Concat(GetResultBuffer(126)).Concat(GetResultBuffer(100)).ToArray();
            //almost 3 responses, just 1 byte of the body left
            var firstSlice = buffer.Length - 1;
            connection.ReadParse(buffer, firstSlice);
            CollectionAssert.AreEqual(new short[] { 127, 126 }, streamIds);
            buffer = buffer.Skip(firstSlice).ToArray();
            connection.ReadParse(buffer, buffer.Length);
            CollectionAssert.AreEqual(new short[] { 127, 126, 100 }, streamIds);
            TestHelper.WaitUntil(() => responses.Count + exceptions.Count == 3, 500, 1000);
            CollectionAssert.IsEmpty(exceptions);
            CollectionAssert.AreEqual(Enumerable.Repeat(ResultResponse.ResultResponseKind.Void, 3), responses.Select(r => ((ResultResponse)r).Kind));
            buffer = GetResultBuffer(1);
            connection.ReadParse(buffer, buffer.Length);
            CollectionAssert.AreEqual(new short[] { 127, 126, 100, 1 }, streamIds);
        }

        [Test]
        public void ReadParse_Handles_UnComplete_Body_In_Multiple_Messages()
        {
            var connectionMock = GetConnectionMock();
            var streamIds = new List<short>();
            var responses = new ConcurrentBag<Response>();
            connectionMock.Setup(c => c.RemoveFromPending(It.IsAny<short>()))
                .Callback<short>(id => streamIds.Add(id))
                .Returns(() => new OperationState((ex, r) => responses.Add(r)));
            var connection = connectionMock.Object;
            var originalBuffer = GetResultBuffer(127).Concat(GetResultBuffer(126)).Concat(GetResultBuffer(100)).ToArray();
            //almost 3 responses, 3 byte of the body left
            var firstSlice = originalBuffer.Length - 3;
            connection.ReadParse(originalBuffer, firstSlice);
            CollectionAssert.AreEqual(new short[] { 127, 126 }, streamIds);
            //2 more bytes, but not enough
            var buffer = originalBuffer.Skip(firstSlice).Take(2).ToArray();
            connection.ReadParse(buffer, buffer.Length);
            CollectionAssert.AreEqual(new short[] { 127, 126}, streamIds);
            //the last byte
            buffer = originalBuffer.Skip(firstSlice + 2).ToArray();
            connection.ReadParse(buffer, buffer.Length);
            CollectionAssert.AreEqual(new short[] { 127, 126, 100 }, streamIds);
            TestHelper.WaitUntil(() => responses.Count == 3);
            CollectionAssert.AreEqual(Enumerable.Repeat(ResultResponse.ResultResponseKind.Void, 3), responses.Select(r => ((ResultResponse)r).Kind));
            buffer = GetResultBuffer(1);
            connection.ReadParse(buffer, buffer.Length);
            CollectionAssert.AreEqual(new short[] { 127, 126, 100, 1 }, streamIds);
        }

        [Test]
        public void ReadParse_Handles_UnComplete_Body_Multiple_Times()
        {
            var connectionMock = GetConnectionMock();
            var streamIds = new List<short>();
            var responses = new ConcurrentBag<Response>();
            connectionMock.Setup(c => c.RemoveFromPending(It.IsAny<short>()))
                .Callback<short>(id => streamIds.Add(id))
                .Returns(() => new OperationState((ex, r) => responses.Add(r)));
            var connection = connectionMock.Object;
            var buffer = GetResultBuffer(127)
                .Concat(GetResultBuffer(126))
                .Concat(GetResultBuffer(100))
                .ToArray();
            //almost 3 responses, 3 byte of the body left
            var length = buffer.Length - 3;
            connection.ReadParse(buffer, length);
            CollectionAssert.AreEqual(new short[] { 127, 126 }, streamIds);
            //the rest of the last message plus a new message
            buffer = buffer.Skip(length).Concat(GetResultBuffer(99)).ToArray();
            length = buffer.Length - 3;
            connection.ReadParse(buffer, length);
            CollectionAssert.AreEqual(new short[] { 127, 126, 100 }, streamIds);

            buffer = buffer.Skip(length).Concat(GetResultBuffer(98)).ToArray();
            length = buffer.Length - 3;
            connection.ReadParse(buffer, length);
            CollectionAssert.AreEqual(new short[] { 127, 126, 100, 99 }, streamIds);

            buffer = buffer.Skip(length).Concat(GetResultBuffer(97)).ToArray();
            length = buffer.Length;
            connection.ReadParse(buffer, length);
            CollectionAssert.AreEqual(new short[] { 127, 126, 100, 99, 98, 97 }, streamIds);
            TestHelper.WaitUntil(() => responses.Count == 6);
            CollectionAssert.AreEqual(Enumerable.Repeat(ResultResponse.ResultResponseKind.Void, 6), responses.Select(r => ((ResultResponse)r).Kind));
            buffer = GetResultBuffer(1);
            connection.ReadParse(buffer, buffer.Length);
            CollectionAssert.AreEqual(new short[] { 127, 126, 100, 99, 98, 97, 1 }, streamIds);
        }

        [Test]
        public void ReadParse_Handles_UnComplete_Body_With_Following_Frames()
        {
            var connectionMock = GetConnectionMock();
            var streamIds = new List<short>();
            var responses = new ConcurrentBag<Response>();
            connectionMock.Setup(c => c.RemoveFromPending(It.IsAny<short>()))
                .Callback<short>(id => streamIds.Add(id))
                .Returns(() => new OperationState((ex, r) => responses.Add(r)));
            var connection = connectionMock.Object;
            var buffer = GetResultBuffer(127).Concat(GetResultBuffer(126)).Concat(GetResultBuffer(100)).ToArray();
            //almost 3 responses, just 1 byte of the body left
            var firstSlice = buffer.Length - 1;
            connection.ReadParse(buffer, firstSlice);
            CollectionAssert.AreEqual(new short[] { 127, 126 }, streamIds);
            buffer = buffer.Skip(firstSlice).Concat(GetResultBuffer(99)).ToArray();
            connection.ReadParse(buffer, buffer.Length);
            CollectionAssert.AreEqual(new short[] { 127, 126, 100, 99 }, streamIds);
            TestHelper.WaitUntil(() => responses.Count == 4);
            CollectionAssert.AreEqual(Enumerable.Repeat(ResultResponse.ResultResponseKind.Void, 4), responses.Select(r => ((ResultResponse)r).Kind));
            buffer = GetResultBuffer(1);
            connection.ReadParse(buffer, buffer.Length);
            CollectionAssert.AreEqual(new short[] { 127, 126, 100, 99, 1 }, streamIds);
        }

        /// <summary>
        /// Gets a buffer containing 8 bytes for header and 4 bytes for the body.
        /// For result + void response message  (protocol v2)
        /// </summary>
        private static byte[] GetResultBuffer(byte streamId = 0)
        {
            return new byte[]
            {
                //header
                0x82, 0, streamId, ResultResponse.OpCode, 0, 0, 0, 4, 
                //body
                0, 0, 0, 1
            };
        }
    }
}