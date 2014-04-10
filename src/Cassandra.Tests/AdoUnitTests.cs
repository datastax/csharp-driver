using Cassandra.Data;
using Moq;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using NUnit.Framework;

namespace Cassandra.IntegrationTests.Data
{
    [TestFixture]
    public class AdoUnitTests
    {
        /// <summary>
        /// Determines that the 
        /// </summary>
        [Test]
        public void CommandExecuteReaderUsesSyncExecute()
        {
            var connection = new CqlConnection();
            var sessionMock = new Mock<ISession>();
            var session = sessionMock.Object;
            var rowset = new RowSet(new OutputVoid(null), session);
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
