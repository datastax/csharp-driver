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

namespace Cassandra.IntegrationTests.Core
{
    [Category("short")]
    public class CustomTypeTests : TestGlobals
    {
        ISession _session = null;

        [SetUp]
        public void SetupTest()
        {
            IndividualTestSetup();
            _session = TestClusterManager.GetTestCluster(1).Session;
        }

        /** Test for paging
            *
            * The simplePagingTest inserts 100 rows into a Cassandra cluster. It then limits the fetch size to be
            * 5 and fetches a page of results, asserting that the results are not fully fetched. Finally, it fetches
            * the remaining pages in batches of 5, asserting along the way that the inserted values match the expected.
            *
            *
            * @expected_errors UnsupportedFeatureException If run on cassandra version < 2.0 (protocol v1)
            * @throws Throwable
            * @since 2.0
            *
            * @expected_result The driver should be able to fetch pages with specified fetchSize, and know
            * when the result is fully fetched.
            *
            * @test_assumptions
            *   - A running Cassandra cluster > 2.0
            *   - Datastax java-driver > 2.0
            *
        */
        [Test]
        public void DynamicCompositeTypeTest()
        {
            string uniqueTableName = TestUtils.GetUniqueTableName();
            string tabledef = "CREATE TABLE " + uniqueTableName + " ("
                              + "    k int,"
                              + "    c 'DynamicCompositeType(s => UTF8Type, i => Int32Type)',"
                              + "    v int,"
                              + "    PRIMARY KEY (k, c)"
                              + ") WITH COMPACT STORAGE";

            _session.WaitForSchemaAgreement(_session.Execute(tabledef));

            _session.Execute("INSERT INTO " + uniqueTableName + "(k, c, v) VALUES (0, 's@foo:i@32', 1)");
            _session.Execute("INSERT INTO " + uniqueTableName + "(k, c, v) VALUES (0, 'i@42', 2)");
            _session.Execute("INSERT INTO " + uniqueTableName + "(k, c, v) VALUES (0, 'i@12:i@3', 3)");

            var rs = _session.Execute("SELECT * FROM " + uniqueTableName);
            {
                IEnumerator<Row> ren = rs.GetRows().GetEnumerator();
                ren.MoveNext();
                Row r = ren.Current;
                Assert.AreEqual(r.GetValue<int>("k"), 0);
                Assert.AreEqual(r.GetValue<byte[]>("c"), serializeForDynamicType(12, 3));
                Assert.AreEqual(r.GetValue<int>("v"), 3);

                ren.MoveNext();
                r = ren.Current;
                Assert.AreEqual(r.GetValue<int>("k"), 0);
                Assert.AreEqual(r.GetValue<byte[]>("c"), serializeForDynamicType(42));
                Assert.AreEqual(r.GetValue<int>("v"), 2);

                ren.MoveNext();
                r = ren.Current;
                Assert.AreEqual(r.GetValue<int>("k"), 0);
                Assert.AreEqual(r.GetValue<byte[]>("c"), serializeForDynamicType("foo", 32));
                Assert.AreEqual(r.GetValue<int>("v"), 1);
            }
        }

        private byte[] serializeForDynamicType(params object[] vals)
        {
            var elt = new BEBinaryWriter();
            foreach (object p in vals)
            {
                if (p is int)
                {
                    elt.WriteUInt16(0x8000 | 'i');
                    elt.WriteUInt16(4);
                    elt.WriteInt32((int)p);
                    elt.WriteByte(0);
                }
                else if (p is String)
                {
                    elt.WriteUInt16(0x8000 | 's');
                    elt.WriteString(p as string);
                    elt.WriteByte(0);
                }
                else
                {
                    throw new InvalidOperationException();
                }
            }
            var ret = new byte[elt.Length];
            Buffer.BlockCopy(elt.GetBuffer(), 0, ret, 0, (int)elt.Length);
            return ret;
        }

    }
}
