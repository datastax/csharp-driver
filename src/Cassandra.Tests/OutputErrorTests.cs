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

using NUnit.Framework;
using Assert = NUnit.Framework.Legacy.ClassicAssert;

namespace Cassandra.Tests
{
    /// <summary>
    /// Tests for the Cassandra.OutputError class.
    /// </summary>
    [TestFixture]
    public class OutputErrorTests
    {
        [Test]
        public void Should_Throw_Helpful_Exception_On_Unknown_Error_Code()
        {
            int unknownCode = int.MaxValue;
            string unknownMessage = "This message should be included in the exception";
            FrameReader frameReader = null; // This isn't currently needed to reproduce so set as null

            // Make sure we get an exception
            var caught = Assert.Catch<DriverInternalError>(() => OutputError.CreateOutputError(unknownCode, unknownMessage, frameReader));

            // Verify the exception message contains the code and the message from the server
            Assert.True(caught.Message.Contains(unknownCode.ToString()));
            Assert.True(caught.Message.Contains(unknownMessage));
        }
    }
}
