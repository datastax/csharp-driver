using Cassandra.Data;
using Moq;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
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
            var session = sessionMock.Object;
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
