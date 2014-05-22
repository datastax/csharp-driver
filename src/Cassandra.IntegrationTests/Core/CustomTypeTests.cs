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

using NUnit.Framework;
using System;
using System.Collections.Generic;

namespace Cassandra.IntegrationTests.Core
{
    [Category("short")]
    public class CustomTypeTests : SingleNodeClusterTest
    {
        private byte[] serializeForDynamicType(params object[] vals)
        {
            var elt = new BEBinaryWriter();
            foreach (object p in vals)
            {
                if (p is int)
                {
                    elt.WriteUInt16(0x8000 | 'i');
                    elt.WriteUInt16(4);
                    elt.WriteInt32((int) p);
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
            Buffer.BlockCopy(elt.GetBuffer(), 0, ret, 0, (int) elt.Length);
            return ret;
        }

        [Test]
        public void DynamicCompositeTypeTest()
        {
            string tabledef = "CREATE TABLE test ("
                              + "    k int,"
                              + "    c 'DynamicCompositeType(s => UTF8Type, i => Int32Type)',"
                              + "    v int,"
                              + "    PRIMARY KEY (k, c)"
                              + ") WITH COMPACT STORAGE";

            Session.WaitForSchemaAgreement(Session.Execute(tabledef));

            Session.Execute("INSERT INTO test(k, c, v) VALUES (0, 's@foo:i@32', 1)");
            Session.Execute("INSERT INTO test(k, c, v) VALUES (0, 'i@42', 2)");
            Session.Execute("INSERT INTO test(k, c, v) VALUES (0, 'i@12:i@3', 3)");

            var rs = Session.Execute("SELECT * FROM test");
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
    }
}