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

using Cassandra.IntegrationTests.TestBase;
using NUnit.Framework;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace Cassandra.IntegrationTests.Core
{
    [Category("short")]
    public class ConsistencyShortTests : TestGlobals
    {
        ISession _session = null;
        const string AllTypesTableName = "all_types_table_serial_consistencyshorttests";

        [SetUp]
        public void SetupFixture()
        {
            _session = TestClusterManager.GetTestCluster(1).Session;
            try
            {
                _session.WaitForSchemaAgreement(_session.Execute(String.Format(TestUtils.CreateTableAllTypes, AllTypesTableName)));
            }
            catch (Cassandra.AlreadyExistsException e) { }
        }

        [Test]
        public void SerialConsistencyTest()
        {
            //You can not specify local serial consistency as a valid read one.
            Assert.Throws<RequestInvalidException>(() =>
            {
                _session.Execute("SELECT * FROM " + AllTypesTableName, ConsistencyLevel.LocalSerial);
            });

            //It should work
            var statement = new SimpleStatement("SELECT * FROM " + AllTypesTableName)
                .SetConsistencyLevel(ConsistencyLevel.Quorum)
                .SetSerialConsistencyLevel(ConsistencyLevel.LocalSerial);
            //Read consistency specified and write consistency specified
            _session.Execute(statement);

            //You can not specify serial consistency as a valid read one.
            Assert.Throws<RequestInvalidException>(() =>
            {
                _session.Execute("SELECT * FROM " + AllTypesTableName, ConsistencyLevel.Serial);
            });
        }

        [Test]
        public void LocalOneIsValidConsistencyTest()
        {
            //Local One is a valid read consistency
            Assert.DoesNotThrow(() => _session.Execute("SELECT * FROM " + AllTypesTableName, ConsistencyLevel.LocalOne));

            Assert.Throws<ArgumentException>(() =>
            {
                //You can not specify local serial consistency as a valid read one.
                var statement = new SimpleStatement("SELECT * FROM " + AllTypesTableName)
                    .SetConsistencyLevel(ConsistencyLevel.Quorum)
                    .SetSerialConsistencyLevel(ConsistencyLevel.LocalOne);
                //Read consistency specified and write consistency specified
                _session.Execute(statement);
            });
        }

        [Test]
        public void PreparedStatementConsistencyShouldBeMantainedWhenBound()
        {
            var ps = _session.Prepare("SELECT * FROM " + AllTypesTableName);
            var rs = _session.Execute(ps.SetConsistencyLevel(ConsistencyLevel.Quorum).Bind());
            Assert.AreEqual(ConsistencyLevel.Quorum, rs.Info.AchievedConsistency);
        }
    }
}
