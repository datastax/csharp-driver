using NUnit.Framework;
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
