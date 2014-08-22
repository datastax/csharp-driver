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

﻿using NUnit.Framework;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;

namespace Cassandra.Tests
{
    [TestFixture]
    public class IOUnitTests
    {
        [Test]
        public void ListBackedStreamReads()
        {
            var stream = new ListBackedStream();
            var writeBuffer = new byte[100];
            for (byte i = 1; i < 100; i++)
            {
                writeBuffer[i] = i;
            }
            stream.Write(writeBuffer, 0, 100);
            stream.Write(writeBuffer, 0, 100);
            stream.Write(writeBuffer, 0, 50);
            Assert.AreEqual(250, stream.Length);
            stream.Position = 0;

            var buffer = new byte[220];
            stream.Read(buffer, 0, buffer.Length);
            Assert.True(buffer.Take(100).SequenceEqual(writeBuffer), "The buffers do not contain the same values");
            Assert.True(buffer.Skip(100).Take(100).SequenceEqual(writeBuffer), "The buffers do not contain the same values");
            Assert.True(buffer.Skip(200).Take(20).SequenceEqual(writeBuffer.Take(20)), "The buffers do not contain the same values");

            stream = new ListBackedStream();
            stream.Write(writeBuffer, 50, 50);
            stream.Position = 0;

            buffer = new byte[50];
            stream.Read(buffer, 0, buffer.Length);
            Assert.True(buffer.SequenceEqual(writeBuffer.Skip(50).Take(50)), "The buffers do not contain the same values");

            stream = new ListBackedStream();
            stream.Write(writeBuffer, 0, 100);
            stream.Position = 0;
            Assert.AreEqual(100, stream.Length);

            buffer = new byte[10];
            stream.Read(buffer, 0, buffer.Length);
            stream.Read(buffer, 0, buffer.Length);
            Assert.AreEqual(buffer, writeBuffer.Skip(10).Take(10));
        }

        [Test]
        public void OperationStateAppendsBuffers()
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
            var operationState = new OperationState();
            operationState.Header = header;
            operationState.AppendBody(writeBuffer, 0, 256);

            operationState.BodyStream.Position = 0;
            operationState.BodyStream.Read(readBuffer, 0, 256);
            Assert.AreEqual(writeBuffer, readBuffer);


            operationState = new OperationState();
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
    }
}
