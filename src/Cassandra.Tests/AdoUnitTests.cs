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
using Cassandra.Data;
using Moq;
using NUnit.Framework;

namespace Cassandra.Tests
{
    [TestFixture]
    public class AdoUnitTests
    {
        /// <summary>
        /// Determines that the CqlCommand.ExecuteReader method uses Session.Execute sync method.
        /// </summary>
        [Test]
        public void CommandExecuteReaderUsesSyncExecute()
        {
            var connection = new CqlConnection();
            var sessionMock = new Mock<ISession>();
            var rowset = new RowSet();
            sessionMock
                .Setup(s => s.Execute(It.IsAny<string>(), It.IsAny<ConsistencyLevel>()))
                .Returns(rowset)
                .Verifiable();
            connection.ManagedConnection = sessionMock.Object;

            var cmd = (CqlCommand) connection.CreateCommand();
            cmd.CommandText = "INSERT INTO dummy_cf (a,b) VALUES (1,2)";
            var reader = cmd.ExecuteReader();
            reader.Dispose();
            sessionMock.Verify();
        }
    }
}