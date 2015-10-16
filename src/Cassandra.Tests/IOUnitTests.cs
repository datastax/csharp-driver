//
//      Copyright (C) 2012-2014 DataStax Inc.
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
using System.Runtime.InteropServices;
using System.Threading;
using System.Threading.Tasks;
using Cassandra.Tasks;
using Microsoft.IO;

namespace Cassandra.Tests
{
    [TestFixture]
    public class IOUnitTests
    {
        [Test]
        public void OperationState_Appends_Buffers()
        {
            var readBuffer = new byte[256];
            var writeBuffer = new byte[256];
            for (byte i = 1; i < 255; i++)
            {
                writeBuffer[i] = i;
            }
            var header = new FrameHeader
            {
                //256 bytes
                Len = new byte[] { 0, 0, 1, 0}
            };
            var operationState = new OperationState((ex, r) => { }, new RecyclableMemoryStreamManager());
            operationState.Header = header;
            operationState.AppendBody(writeBuffer, 0, 256);

            operationState.BodyStream.Position = 0;
            operationState.BodyStream.Read(readBuffer, 0, 256);
            Assert.AreEqual(writeBuffer, readBuffer);


            operationState = new OperationState((ex, r) => { }, new RecyclableMemoryStreamManager());
            operationState.Header = header;
            operationState.AppendBody(writeBuffer, 0, 100);
            operationState.AppendBody(writeBuffer, 100, 100);
            operationState.AppendBody(writeBuffer, 200, 50);
            operationState.AppendBody(writeBuffer, 250, 6);

            operationState.BodyStream.Position = 0;
            operationState.BodyStream.Read(readBuffer, 0, 256);
            Assert.AreEqual(writeBuffer, readBuffer);

            operationState.BodyStream.Position = 0;
            operationState.BodyStream.Read(readBuffer, 0, 128);
            operationState.BodyStream.Read(readBuffer, 128, 128);
            Assert.AreEqual(writeBuffer, readBuffer);
        }

        [Test]
        public void OperationState_Can_Concurrently_Get_Timeout_And_Response()
        {
            var counter = 0;
            var timedOutReceived = 0;
            TestHelper.Invoke(() =>
            {
                var clientCallbackCounter = 0;
                Action<Exception, AbstractResponse> clientCallback = (ex, r) =>
                {
                    Interlocked.Increment(ref clientCallbackCounter);
                };
                var state = new OperationState(clientCallback, new RecyclableMemoryStreamManager());
                var actions = new Action[]
                {
                    () => state.SetTimedOut(new OperationTimedOutException(new IPEndPoint(0, 1), 200), () => Interlocked.Increment(ref timedOutReceived)),
                    () => { state.InvokeCallback(null); }
                };
                if ((counter++)%2 == 0)
                {
                    //invert order
                    actions = actions.Reverse().ToArray();
                }
                TestHelper.ParallelInvoke(actions);
                //Allow callbacks to be called using the default scheduler
                Thread.Sleep(20);
                Assert.AreEqual(1, clientCallbackCounter);
            }, 50);
            Trace.WriteLine(timedOutReceived);
        }

        [Test]
        public void OperationState_Cancel_Should_Never_Callback_Client()
        {
            var clientCallbackCounter = 0;
            Action<Exception, AbstractResponse> clientCallback = (ex, r) =>
            {
                Interlocked.Increment(ref clientCallbackCounter);
            };
            var state = new OperationState(clientCallback, new RecyclableMemoryStreamManager());
            state.Cancel();
            state.InvokeCallback(null);
            //Allow callbacks to be called using the default scheduler
            Thread.Sleep(20);
            Assert.AreEqual(0, clientCallbackCounter);
        }

        [Test]
        public void TaskHelper_Continue_Does_Not_Call_Synchonization_Post()
        {
            var ctxt = new LockSynchronisationContext();
            SynchronizationContext.SetSynchronizationContext(ctxt);
            var task = TestHelper.DelayedTask(new RowSet(), 1000);
            var cTask = task
                .Continue((rs) => TestHelper.DelayedTask(2, 500).Result)
                .Continue(s => "last one");

            ctxt.Post(state =>
            {
                cTask.Wait(3000);
                Assert.AreEqual(cTask.Status, TaskStatus.RanToCompletion);
            }, null);
        }

        [Test]
        public void BeBinaryWriter_Close_Sets_Frame_Body_Length()
        {
            const int frameLength = 10;
            const int iterations = 8;
            var bufferPool = new RecyclableMemoryStreamManager();
            using (var stream = bufferPool.GetStream("test"))
            {
                for (var i = 0; i < iterations; i++)
                {
                    var writer = new FrameWriter(stream);
                    writer.WriteFrameHeader(2, 0, 127, 8);
                    writer.WriteInt16(Convert.ToInt16(0x0900 + i));
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
