//
//  Copyright (C) 2017 DataStax, Inc.
//
//  Please see the license for details:
//  http://www.datastax.com/terms/datastax-dse-driver-license-terms
//

using Dse.Test.Integration.TestClusterManagement;
using NUnit.Framework;
using System;
using System.Collections.Generic;
using System.IO;
using Dse.Serialization;

namespace Dse.Test.Integration.Core
{
    [Category("short")]
    public class CustomTypeTests : SharedClusterTest
    {
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

            Session.Execute(tabledef);

            Session.Execute("INSERT INTO " + uniqueTableName + "(k, c, v) VALUES (0, 's@foo:i@32', 1)");
            Session.Execute("INSERT INTO " + uniqueTableName + "(k, c, v) VALUES (0, 'i@42', 2)");
            Session.Execute("INSERT INTO " + uniqueTableName + "(k, c, v) VALUES (0, 'i@12:i@3', 3)");

            var rs = Session.Execute("SELECT * FROM " + uniqueTableName);
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
            var elt = new FrameWriter(new MemoryStream(), new Serializer(ProtocolVersion.V1));
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
