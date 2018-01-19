// 
//       Copyright (C) DataStax Inc.
// 
//    Licensed under the Apache License, Version 2.0 (the "License");
//    you may not use this file except in compliance with the License.
//    You may obtain a copy of the License at
// 
//       http://www.apache.org/licenses/LICENSE-2.0
// 
//    Unless required by applicable law or agreed to in writing, software
//    distributed under the License is distributed on an "AS IS" BASIS,
//    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//    See the License for the specific language governing permissions and
//    limitations under the License.
//

using System.IO;
using System.Linq;
using System.Text;
using Cassandra.Responses;
using Cassandra.Serialization;
using NUnit.Framework;
using HeaderFlag = Cassandra.FrameHeader.HeaderFlag;

namespace Cassandra.Tests
{
    [TestFixture]
    public class FrameParserTests
    {
        private const ProtocolVersion Version = ProtocolVersion.MaxSupported;
        private static readonly Serializer Serializer = new Serializer(Version);
        
        [Test]
        public void Should_Parse_ErrorResponse_Of_Syntax_Error()
        {
            var body = GetErrorBody(0x2000, "Test syntax error");
            var header = FrameHeader.ParseResponseHeader(Version, GetHeaderBuffer(body.Length), 0);
            var response = FrameParser.Parse(new Frame(header, new MemoryStream(body), Serializer));
            var ex = IsErrorResponse<SyntaxError>(response);
            Assert.AreEqual("Test syntax error", ex.Message);
        }
        
        [Test]
        public void Should_Parse_ErrorResponse_With_Warnings()
        {
            // Protocol warnings are [string list]: A [short] n, followed by n [string]
            var warningBuffers = BeConverter.GetBytes((ushort)1).Concat(GetProtocolString("Test warning"));
            var body = warningBuffers.Concat(GetErrorBody(0x2000, "Test syntax error")).ToArray();
            var header = FrameHeader.ParseResponseHeader(Version, GetHeaderBuffer(body.Length, HeaderFlag.Warning), 0);
            var response = FrameParser.Parse(new Frame(header, new MemoryStream(body), Serializer));
            var ex = IsErrorResponse<SyntaxError>(response);
            Assert.AreEqual("Test syntax error", ex.Message);
        }

        private static byte[] GetHeaderBuffer(int length, HeaderFlag flags = 0)
        {
            var headerBuffer = new byte[] {0x80 | (int) Version, (byte) flags, 0, 0, ErrorResponse.OpCode}
                .Concat(BeConverter.GetBytes(length));
            return headerBuffer.ToArray();
        }

        private static byte[] GetErrorBody(int code, string message)
        {
            // error body = [int] [string]
            return BeConverter.GetBytes(code).Concat(GetProtocolString(message)).ToArray();
        }

        private static byte[] GetProtocolString(string value)
        {
            var textBuffer = Encoding.UTF8.GetBytes(value);
            return BeConverter.GetBytes((ushort) textBuffer.Length).Concat(textBuffer).ToArray();
        }

        private static TException IsErrorResponse<TException>(Response response) where TException : DriverException
        {
            Assert.IsInstanceOf<ErrorResponse>(response);
            var ex = ((ErrorResponse) response).Output.CreateException();
            Assert.IsInstanceOf<TException>(ex);
            return (TException) ex;
        }
    }
}