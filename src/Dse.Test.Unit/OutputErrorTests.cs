//
//  Copyright (C) 2017 DataStax, Inc.
//
//  Please see the license for details:
//  http://www.datastax.com/terms/datastax-dse-driver-license-terms
//

using NUnit.Framework;

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
