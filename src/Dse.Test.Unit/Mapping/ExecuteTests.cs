//
//  Copyright (C) DataStax, Inc.
//
//  Please see the license for details:
//  http://www.datastax.com/terms/datastax-dse-driver-license-terms
//

using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Dse.Mapping;
using Dse.Tasks;
using Dse.Test.Unit.Mapping.Pocos;
using Dse.Test.Unit.Mapping.TestData;
using Moq;

using NUnit.Framework;

namespace Dse.Test.Unit.Mapping
{
    [TestFixture]
    public class ExecuteTests : MappingTestBase
    {
        [Test]
        public void ExecuteAsync_Sets_Consistency()
        {
            ConsistencyLevel? consistency = null;
            ConsistencyLevel? serialConsistency = null;
            var sessionMock = new Mock<ISession>(MockBehavior.Strict);
            sessionMock.Setup(s => s.Keyspace).Returns<string>(null);
            sessionMock
                .Setup(s => s.ExecuteAsync(It.IsAny<BoundStatement>()))
                .Callback<IStatement>(b =>
                {
                    consistency = b.ConsistencyLevel;
                    serialConsistency = b.SerialConsistencyLevel;
                })
                .Returns(() => TaskHelper.ToTask(new RowSet()))
                .Verifiable();
            sessionMock
                .Setup(s => s.PrepareAsync(It.IsAny<string>()))
                .Returns(TaskHelper.ToTask(GetPrepared()))
                .Verifiable();
            sessionMock
                .Setup(s => s.ExecuteAsync(It.IsAny<BoundStatement>(), It.IsAny<string>()))
                .Callback<IStatement, string>((b, profile) =>
                {
                    consistency = b.ConsistencyLevel;
                    serialConsistency = b.SerialConsistencyLevel;
                })
                .Returns(() => TaskHelper.ToTask(new RowSet()))
                .Verifiable();
            sessionMock
                .Setup(s => s.PrepareAsync(It.IsAny<string>()))
                .Returns(TaskHelper.ToTask(GetPrepared()))
                .Verifiable();
            var mapper = GetMappingClient(sessionMock);
            mapper.ExecuteAsync(new Cql("UPDATE").WithOptions(o => o.SetConsistencyLevel(ConsistencyLevel.EachQuorum).SetSerialConsistencyLevel(ConsistencyLevel.Serial))).Wait();
            Assert.AreEqual(ConsistencyLevel.EachQuorum, consistency);
            Assert.AreEqual(ConsistencyLevel.Serial, serialConsistency);
        }

        [Test]
        public void Execute_Batch_Returns_WhenResponse_IsReceived()
        {
            var newUser = new InsertUser
            {
                Id = Guid.NewGuid(),
                Name = "Dummy"
            };

            var rowsetReturned = false;
            var sessionMock = new Mock<ISession>(MockBehavior.Strict);
            sessionMock.Setup(s => s.Keyspace).Returns<string>(null);
            sessionMock
                .Setup(s => s.ExecuteAsync(It.IsAny<BatchStatement>(), It.IsAny<string>()))
                .Returns(TestHelper.DelayedTask(new RowSet(), 2000).ContinueWith(t =>
                {
                    rowsetReturned = true;
                    return t.Result;
                }))
                .Verifiable();
            sessionMock
                .Setup(s => s.PrepareAsync(It.IsAny<string>()))
                .Returns<string>((cql) => TaskHelper.ToTask(GetPrepared(cql)))
                .Verifiable();
            var mapper = GetMappingClient(sessionMock);
            var batch = mapper.CreateBatch();
            batch.Insert(newUser);
            //Execute
            mapper.Execute(batch);
            Assert.True(rowsetReturned);
            sessionMock.Verify();
        }
    }
}