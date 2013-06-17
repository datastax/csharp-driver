using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

#if MYTEST
using MyTest;
#else
using Microsoft.VisualStudio.TestTools.UnitTesting;
#endif
using System.Text;
using System.Threading;
using System.Globalization;
using System.IO;

namespace Cassandra.MSTest
{

    public class CustomTypeTests
    {
        string Keyspace = "tester";
        Cluster Cluster;
        Session Session;
        CCMBridge.CCMCluster CCMCluster;


        public CustomTypeTests()
        {
        }

        [TestInitialize]
        public void SetFixture()
        {
            Thread.CurrentThread.CurrentCulture = CultureInfo.CreateSpecificCulture("en-US");
            CCMCluster = CCMBridge.CCMCluster.Create(2, Cluster.Builder());
            Session = CCMCluster.Session;
            Cluster = CCMCluster.Cluster;
            Session.CreateKeyspaceIfNotExists(Keyspace);
            Session.ChangeKeyspace(Keyspace);

            var tabledef = "CREATE TABLE test ("
            + "    k int,"
            + "    c 'DynamicCompositeType(s => UTF8Type, i => Int32Type)',"
            + "    v int,"
            + "    PRIMARY KEY (k, c)"
            + ") WITH COMPACT STORAGE";

            Cluster.WaitForSchemaAgreement(Session.Execute(tabledef).QueriedHost);
        }

        [TestCleanup]
        public void Dispose()
        {
            CCMCluster.Discard();
        }


        private byte[] serializeForDynamicType(params object[] vals) {

            BEBinaryWriter elt = new BEBinaryWriter();
            foreach (var p in vals) {
                if (p is int) {
                    elt.WriteUInt16((ushort)(0x8000 |'i'));
                    elt.WriteUInt16((ushort) 4);
                    elt.WriteInt32((int)p);
                    elt.WriteByte((byte)0);
                } else if (p is String) {
                    elt.WriteUInt16((ushort)(0x8000 |'s'));
                    elt.WriteString(p as string);
                    elt.WriteByte((byte)0);
                } else {
                    throw new InvalidOperationException();
                }
            }
            var ret = new byte[elt.Length];
            Buffer.BlockCopy(elt.GetBuffer(), 0, ret, 0, (int) elt.Length);
            return ret;
        }

        [TestMethod]
        [WorksForMe]
        public void DynamicCompositeTypeTest()
        {

            Session.Execute("INSERT INTO test(k, c, v) VALUES (0, 's@foo:i@32', 1)");
            Session.Execute("INSERT INTO test(k, c, v) VALUES (0, 'i@42', 2)");
            Session.Execute("INSERT INTO test(k, c, v) VALUES (0, 'i@12:i@3', 3)");

            var rs = Session.Execute("SELECT * FROM test");

            var ren = rs.GetRows().GetEnumerator();
            ren.MoveNext();
            var r = ren.Current;
            Assert.Equal(r.GetValue<int>("k"), 0);
            Assert.ArrEqual(r.GetValue<byte[]>("c"), serializeForDynamicType(12, 3));
            Assert.Equal(r.GetValue<int>("v"), 3);

            ren.MoveNext();
            r = ren.Current;
            Assert.Equal(r.GetValue<int>("k"), 0);
            Assert.ArrEqual(r.GetValue<byte[]>("c"), serializeForDynamicType(42));
            Assert.Equal(r.GetValue<int>("v"), 2);

            ren.MoveNext();
            r = ren.Current;
            Assert.Equal(r.GetValue<int>("k"), 0);
            Assert.ArrEqual(r.GetValue<byte[]>("c"), serializeForDynamicType("foo", 32));
            Assert.Equal(r.GetValue<int>("v"), 1);

        }
    }
}