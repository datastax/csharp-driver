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
using System.Diagnostics;
using System.IO;
using NUnit.Framework;
using System.Linq;
using System.Net;
using System.Threading;
using System.Threading.Tasks;
using Cassandra.Compression;
using Cassandra.Responses;
using Cassandra.Serialization;
using Cassandra.Tasks;
using Microsoft.IO;

namespace Cassandra.Tests
{
    [TestFixture]
    public class IOUnitTests
    {
        [Test]
        public void OperationState_Can_Concurrently_Get_Timeout_And_Response()
        {
            var counter = 0;
            var timedOutReceived = 0;
            var clientCallbackCounter = 0;
            const int times = 40;
            var expectedTimedout = 0;
            TestHelper.Invoke(() =>
            {
                Action<Exception, Response> clientCallback = (ex, r) =>
                {
                    Interlocked.Increment(ref clientCallbackCounter);
                };
                var state = new OperationState(clientCallback);
                var actions = new Action[]
                {
                    () =>
                    {
                        var timedout = state.MarkAsTimedOut(
                            new OperationTimedOutException(new IPEndPoint(0, 1), 200), () => Interlocked.Increment(ref timedOutReceived));
                        Interlocked.Add(ref expectedTimedout, timedout ? 1 : 0);
                    },
                    () =>
                    {
                        state.InvokeCallback(null);
                    }
                };
                if ((counter++)%2 == 0)
                {
                    //invert order
                    actions = actions.Reverse().ToArray();
                }
                TestHelper.ParallelInvoke(actions);
            }, times);
            //Allow callbacks to be called using the default scheduler
            Thread.Sleep(1000);
            Assert.AreEqual(times, clientCallbackCounter);
            Assert.AreEqual(expectedTimedout, timedOutReceived);
        }

        [Test]
        public void OperationState_Can_Concurrently_Get_Calls_To_SetCompleted()
        {
            var counter = 0;
            var clientCallbackCounter = 0L;
            const int times = 40;
            TestHelper.Invoke(() =>
            {
                Action<Exception, Response> clientCallback = (ex, r) =>
                {
                    // ReSharper disable once AccessToModifiedClosure
                    Interlocked.Increment(ref clientCallbackCounter);
                };
                var state = new OperationState(clientCallback);
                var actions = Enumerable.Repeat<Action>(() =>
                {
                    var cb = state.SetCompleted();
                    cb(null, null);
                }, 2);
                if ((counter++) % 2 == 0)
                {
                    //invert order
                    actions = actions.Reverse();
                }
                TestHelper.ParallelInvoke(actions.ToArray());
            }, times);
            //Allow callbacks to be called using the default scheduler
            Thread.Sleep(1000);
            Assert.AreEqual(times, Interlocked.Read(ref clientCallbackCounter));
        }

        [Test]
        public void OperationState_Cancel_Should_Never_Callback_Client()
        {
            var clientCallbackCounter = 0;
            Action<Exception, Response> clientCallback = (ex, r) =>
            {
                Interlocked.Increment(ref clientCallbackCounter);
            };
            var state = new OperationState(clientCallback);
            state.Cancel();
            state.InvokeCallback(null);
            //Allow callbacks to be called using the default scheduler
            Thread.Sleep(20);
            Assert.AreEqual(0, clientCallbackCounter);
        }

        [Test]
        public void BeBinaryWriter_Close_Sets_Frame_Body_Length()
        {
            const int frameLength = 10;
            const int iterations = 8;
            const ProtocolVersion protocolVersion = ProtocolVersion.V2;
            var bufferPool = new RecyclableMemoryStreamManager();
            using (var stream = bufferPool.GetStream("test"))
            {
                for (var i = 0; i < iterations; i++)
                {
                    var writer = new FrameWriter(stream, new Serializer(protocolVersion));
                    writer.WriteFrameHeader(0, 127, 8);
                    writer.WriteUInt16(Convert.ToUInt16(0x0900 + i));
                    var length = writer.Close();
                    Assert.AreEqual(frameLength, length);
                }
                Assert.AreEqual(frameLength * iterations, stream.Length);
                for (byte i = 0; i < iterations; i++)
                {
                    var buffer = new byte[frameLength];
                    stream.Position = i * frameLength;
                    stream.Read(buffer, 0, frameLength);
                    CollectionAssert.AreEqual(new byte[] { 2, 0, 127, 8, 0, 0, 0, 2, 9, i}, buffer);
                }
            }
        }

        [Test]
        public void RecyclableMemoryStream_GetBufferList_Handles_Multiple_Blocks()
        {
            const int blockSize = 16;
            var buffer = new byte[256];
            for (var i = 0; i < buffer.Length; i++)
            {
                buffer[i] = (byte) i;
            }
            var bufferPool = new RecyclableMemoryStreamManager(blockSize, 1024, 1024 * 1024 * 10);
            using (var stream = (RecyclableMemoryStream)bufferPool.GetStream())
            {
                stream.Write(buffer, 0, 12);
                CollectionAssert.AreEqual(new[] { new ArraySegment<byte>(buffer, 0, 12) }, stream.GetBufferList());
            }
            using (var stream = (RecyclableMemoryStream)bufferPool.GetStream())
            {
                stream.Write(buffer, 0, blockSize);
                var bufferList = stream.GetBufferList();
                Assert.AreEqual(1, bufferList.Count);
                CollectionAssert.AreEqual(new[] { new ArraySegment<byte>(buffer, 0, blockSize) }, bufferList);
            }
            using (var stream = (RecyclableMemoryStream)bufferPool.GetStream())
            {
                stream.Write(buffer, 0, blockSize * 2);
                var bufferList = stream.GetBufferList();
                Assert.AreEqual(2, bufferList.Count);
                CollectionAssert.AreEqual(new[] { new ArraySegment<byte>(buffer, 0, blockSize), new ArraySegment<byte>(buffer, blockSize, blockSize) }, bufferList);
            }
            using (var stream = (RecyclableMemoryStream)bufferPool.GetStream())
            {
                stream.Write(buffer, 0, blockSize * 2 + 1);
                var bufferList = stream.GetBufferList();
                Assert.AreEqual(3, bufferList.Count);
                CollectionAssert.AreEqual(new[]
                {
                    new ArraySegment<byte>(buffer, 0, blockSize),
                    new ArraySegment<byte>(buffer, blockSize, blockSize),
                    new ArraySegment<byte>(buffer, blockSize * 2, 1)
                }, bufferList);
            }
        }

        [Test]
        public void WrappedStream_Tests()
        {
            var memoryStream = new MemoryStream(Enumerable.Range(0, 128).Select(i => (byte)i).ToArray(), false);
            var buffer = new byte[128];
            memoryStream.Position = 20;
            var wrapper = new WrappedStream(memoryStream, 100);
            var read = wrapper.Read(buffer, 0, 3);
            CollectionAssert.AreEqual(new byte[] { 20, 21, 22 }, buffer.Take(read));
            //do not pass the length of the wrapper
            memoryStream.Position = 10;
            wrapper = new WrappedStream(memoryStream, 2);
            read = wrapper.Read(buffer, 0, 100);
            CollectionAssert.AreEqual(new byte[] { 10, 11 }, buffer.Take(read));
            //do not pass the length of the internal stream
            memoryStream.Position = 126;
            wrapper = new WrappedStream(memoryStream, 2);
            read = wrapper.Read(buffer, 0, 100);
            CollectionAssert.AreEqual(new byte[] { 126, 127 }, buffer.Take(read));

        }

        class LockSynchronisationContext : SynchronizationContext
        {
            private readonly object _postLock = new object();

            public override void Send(SendOrPostCallback codeToRun, object state)
            {
                throw new NotImplementedException();
            }

            public override void Post(SendOrPostCallback codeToRun, object state)
            {
                lock (_postLock)
                {
                    codeToRun(state);
                }
            }

        }
    }
}
