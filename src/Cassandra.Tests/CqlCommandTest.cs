//
//      Copyright (C) 2012 DataStax Inc.
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
using System.Data;
using Cassandra.Data;
using NUnit.Framework;

namespace Cassandra.IntegrationTests.Data
{
    [TestFixture]
    public class CqlCommandTest
    {
        [Test]
        public void TestCqlCommand()
        {
            var target = new CqlCommand();

            // test CreateDbParameter()
            var parameter = target.CreateParameter();
            Assert.IsNotNull(parameter);

            // test Parameters
            var parameterCollection = target.Parameters;
            Assert.IsNotNull(parameterCollection);
            Assert.AreEqual(parameterCollection, target.Parameters);

            // test Connection
            var connection = new CqlConnection("contact points=127.0.0.1;port=9042");
            Assert.IsNull(target.Connection);
            target.Connection = connection;
            Assert.AreEqual(connection, target.Connection);

            // test IsPrepared
            Assert.IsTrue(target.IsPrepared);

            // test CommandText
            var cqlQuery = "test query";
            Assert.IsNull(target.CommandText);
            target.CommandText = cqlQuery;
            Assert.AreEqual(cqlQuery, target.CommandText);

            // test CommandTimeout, it should always return -1
            var timeout = 1;
            Assert.AreEqual(-1, target.CommandTimeout);
            target.CommandTimeout = timeout;
            Assert.AreEqual(-1, target.CommandTimeout);

            // test CommandType, it should always return CommandType.Text
            var commandType = CommandType.TableDirect;
            Assert.AreEqual(CommandType.Text, target.CommandType);
            target.CommandType = commandType;
            Assert.AreEqual(CommandType.Text, target.CommandType);

            // test DesignTimeVisible, it should always return true
            Assert.IsTrue(target.DesignTimeVisible);
            target.DesignTimeVisible = false;
            Assert.IsTrue(target.DesignTimeVisible);

            // test UpdateRowSource, it should always return UpdateRowSource.FirstReturnedRecord
            var updateRowSource = UpdateRowSource.Both;
            Assert.AreEqual(UpdateRowSource.FirstReturnedRecord, target.UpdatedRowSource);
            target.UpdatedRowSource = updateRowSource;
            Assert.AreEqual(UpdateRowSource.FirstReturnedRecord, target.UpdatedRowSource);
        }

        [Test]
        [ExpectedException(typeof(InvalidOperationException))]
        public void TestCqlCommand_Prepare_Without_Connection()
        {
            var target = new CqlCommand();
            target.Parameters.Add("p1", "1");
            target.Prepare();
        }
    }

}
