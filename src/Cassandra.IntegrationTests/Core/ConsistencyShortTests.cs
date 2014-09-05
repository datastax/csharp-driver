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
using System.Linq;
using System.Text;

namespace Cassandra.IntegrationTests.Core
{
    [Category("short")]
    public class ConsistencyShortTests : SingleNodeClusterTest
    {
        const string AllTypesTableName = "all_types_table_serial";
        public override void TestFixtureSetUp()
        {
            base.TestFixtureSetUp();

            Session.WaitForSchemaAgreement(Session.Execute(String.Format(TestUtils.CREATE_TABLE_ALL_TYPES, AllTypesTableName)));
        }

        [Test]
        public void SerialConsistencyTest()
        {
            //You can not specify local serial consistency as a valid read one.
            Assert.Throws<RequestInvalidException>(() =>
            {
                Session.Execute("SELECT * FROM " + AllTypesTableName, ConsistencyLevel.LocalSerial);
            });

            //It should work
            var statement = new SimpleStatement("SELECT * FROM " + AllTypesTableName)
                .SetConsistencyLevel(ConsistencyLevel.Quorum)
                .SetSerialConsistencyLevel(ConsistencyLevel.LocalSerial);
            //Read consistency specified and write consistency specified
            Session.Execute(statement);

            //You can not specify serial consistency as a valid read one.
            Assert.Throws<RequestInvalidException>(() =>
            {
                Session.Execute("SELECT * FROM " + AllTypesTableName, ConsistencyLevel.Serial);
            });
        }

        [Test]
        public void LocalOneIsValidConsistencyTest()
        {
            //Local One is a valid read consistency
            Assert.DoesNotThrow(() => Session.Execute("SELECT * FROM " + AllTypesTableName, ConsistencyLevel.LocalOne));

            Assert.Throws<ArgumentException>(() =>
            {
                //You can not specify local serial consistency as a valid read one.
                var statement = new SimpleStatement("SELECT * FROM " + AllTypesTableName)
                    .SetConsistencyLevel(ConsistencyLevel.Quorum)
                    .SetSerialConsistencyLevel(ConsistencyLevel.LocalOne);
                //Read consistency specified and write consistency specified
                Session.Execute(statement);
            });
        }
    }
}
