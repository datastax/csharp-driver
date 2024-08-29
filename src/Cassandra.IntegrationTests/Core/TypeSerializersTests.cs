//
//      Copyright (C) DataStax Inc.
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
using System.Collections;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Numerics;

using Cassandra.IntegrationTests.TestBase;
using Cassandra.IntegrationTests.TestClusterManagement;
using Cassandra.Serialization;
using Cassandra.Tests;
using Cassandra.Tests.Extensions.Serializers;
using NUnit.Framework;

namespace Cassandra.IntegrationTests.Core
{
    [Category(TestCategory.Short), Category(TestCategory.RealCluster)]
    public class TypeSerializersTests : SharedClusterTest
    {
        private const string DecimalInsertQuery = "INSERT INTO tbl_decimal (id, text_value, value1, value2) VALUES (?, ?, ?, ?)";
        private const string DecimalSelectQuery = "SELECT * FROM tbl_decimal WHERE id = ?";
        private const string CustomInsertQuery = "INSERT INTO tbl_custom (id, value) VALUES (?, ?)";
        private const string CustomSelectQuery = "SELECT * FROM tbl_custom WHERE id = ?";

        private const string CustomTypeName = "org.apache.cassandra.db.marshal.DynamicCompositeType(" +
                                              "s=>org.apache.cassandra.db.marshal.UTF8Type," +
                                              "i=>org.apache.cassandra.db.marshal.Int32Type)";
        
        private const string CustomTypeName2 = "org.apache.cassandra.db.marshal.DynamicCompositeType(" +
                                               "i=>org.apache.cassandra.db.marshal.Int32Type," +
                                               "s=>org.apache.cassandra.db.marshal.UTF8Type)";

        protected override string[] SetupQueries
        {
            get
            {
                return new[]
                {
                    "CREATE TABLE tbl_decimal (id uuid PRIMARY KEY, text_value text, value1 decimal, value2 decimal)",
                    "CREATE TABLE tbl_decimal_key (id decimal PRIMARY KEY)",
                    string.Format("CREATE TABLE tbl_custom (id uuid PRIMARY KEY, value '{0}')", CustomTypeName)
                };
            }
        }

        [Test]
        public void Should_Throw_When_TypeSerializer_Not_Defined()
        {
            var statement = new SimpleStatement(DecimalInsertQuery, Guid.NewGuid(), null, new BigDecimal(1, 100000909), null);
            var ex = Assert.Throws<InvalidTypeException>(() => Session.Execute(statement));
            StringAssert.Contains("CLR", ex.Message);
        }

        [Test]
        public void Should_Use_Primitive_TypeSerializers_With_BoundStatements()
        {
            var builder = ClusterBuilder()
                                 .AddContactPoint(TestCluster.InitialContactPoint)
                                 .WithTypeSerializers(new TypeSerializerDefinitions()
                                     .Define(new BigDecimalSerializer())
                                     .Define(new DummyCustomTypeSerializer()));
            using (var cluster = builder.Build())
            {
                var session = cluster.Connect(KeyspaceName);
                object[][] values =
                {
                    new object[] { Guid.NewGuid(), new BigDecimal(1, BigInteger.Parse("9999999999999999999999999999")), 999999999999999999999999999.9m },
                    new object[] { Guid.NewGuid(), new BigDecimal(6, 367383), 0.367383M },
                    new object[] { Guid.NewGuid(), new BigDecimal(0, 0), 0M },
                    new object[] { Guid.NewGuid(), new BigDecimal(1, -1), -0.1M }
                };
                var ps = session.Prepare(DecimalInsertQuery);
                foreach (var item in values)
                {
                    var id = item[0];
                    //allow inserts as BigDecimal and decimal
                    session.Execute(ps.Bind(id, item[2].ToString(), item[1], item[2]));
                    var row = session.Execute(new SimpleStatement(DecimalSelectQuery, id)).First();
                    //it allows to retrieve only by 1 type
                    Assert.AreEqual(row.GetValue<BigDecimal>("value1"), item[1]);
                    Assert.AreEqual(row.GetValue<BigDecimal>("value2"), item[1]);
                }
            }
        }

        [Test]
        public void Should_Use_Primitive_TypeSerializers_With_SimpleStatements()
        {
            var builder = ClusterBuilder()
                                 .AddContactPoint(TestCluster.InitialContactPoint)
                                 .WithTypeSerializers(new TypeSerializerDefinitions()
                                     .Define(new BigDecimalSerializer())
                                     .Define(new DummyCustomTypeSerializer()));
            using (var cluster = builder.Build())
            {
                var session = cluster.Connect(KeyspaceName);
                object[][] values =
                {
                    new object[] { Guid.NewGuid(), new BigDecimal(1, 100000909), 10000090.9M },
                    new object[] { Guid.NewGuid(), new BigDecimal(5, 367383), 3.67383M },
                    new object[] { Guid.NewGuid(), new BigDecimal(0, 0), 0M },
                    new object[] { Guid.NewGuid(), new BigDecimal(0, -1), -1M }
                };
                foreach (var item in values)
                {
                    var id = item[0];
                    //allow inserts as BigDecimal and decimal
                    var statement = new SimpleStatement(DecimalInsertQuery, id, item[2].ToString(), item[1], item[2]);
                    session.Execute(statement);
                    var row = session.Execute(new SimpleStatement(DecimalSelectQuery, id)).First();
                    //it allows to retrieve only by 1 type
                    Assert.AreEqual(row.GetValue<BigDecimal>("value1"), item[1]);
                    Assert.AreEqual(row.GetValue<BigDecimal>("value2"), item[1]);
                }
            }
        }

        [Test]
        public void Should_Use_Primitive_TypeSerializers_With_Simple_BatchStatements()
        {
            var builder = ClusterBuilder()
                                 .AddContactPoint(TestCluster.InitialContactPoint)
                                 .WithTypeSerializers(new TypeSerializerDefinitions()
                                     .Define(new BigDecimalSerializer())
                                     .Define(new DummyCustomTypeSerializer()));
            using (var cluster = builder.Build())
            {
                var session = cluster.Connect(KeyspaceName);
                object[][] values =
                {
                    new object[] { Guid.NewGuid(), new BigDecimal(2, 9071), 90.71M },
                    new object[] { Guid.NewGuid(), new BigDecimal(5, 367383), 3.67383M },
                    new object[] { Guid.NewGuid(), new BigDecimal(0, 0), 0M },
                    new object[] { Guid.NewGuid(), new BigDecimal(0, -1), -1M }
                };
                var batch = new BatchStatement();
                foreach (var item in values)
                {
                    var id = item[0];
                    batch.Add(new SimpleStatement(DecimalInsertQuery, id, item[2].ToString(), item[1], item[2]));
                }
                session.Execute(batch);
                foreach (var item in values)
                {
                    var id = item[0];
                    var row = session.Execute(new SimpleStatement(DecimalSelectQuery, id)).First();
                    Assert.AreEqual(row.GetValue<BigDecimal>("value1"), item[1]);
                    Assert.AreEqual(row.GetValue<BigDecimal>("value2"), item[1]);
                }
            }
        }

        [Test]
        public void Should_Use_Primitive_TypeSerializers_For_Partition_Key()
        {
            var builder = ClusterBuilder()
                                 .AddContactPoint(TestCluster.InitialContactPoint)
                                 .WithTypeSerializers(new TypeSerializerDefinitions()
                                     .Define(new BigDecimalSerializer())
                                     .Define(new DummyCustomTypeSerializer()));
            using (var cluster = builder.Build())
            {
                var session = cluster.Connect(KeyspaceName);
                var values = new[]
                {
                    new BigDecimal(3, 123),
                    new BigDecimal(6, 367383),
                    new BigDecimal(0, 0),
                    new BigDecimal(1, -1)
                };
                var ps = session.Prepare("INSERT INTO tbl_decimal_key (id) VALUES (?)");
                foreach (var v in values)
                {
                    var statement = ps.Bind(v);
                    Assert.NotNull(statement.RoutingKey);
                    CollectionAssert.AreEqual(new BigDecimalSerializer().Serialize((ushort)session.BinaryProtocolVersion, v), statement.RoutingKey.RawRoutingKey);
                    session.Execute(statement);
                    var row = session.Execute(new SimpleStatement("SELECT * FROM tbl_decimal_key WHERE id = ?", v)).First();
                    Assert.AreEqual(row.GetValue<BigDecimal>("id"), v);
                }
            }
        }

        [Test]
        public void Should_Use_Custom_TypeSerializers()
        {
            var typeSerializerName = TestClusterManager.CheckDseVersion(new Version(6, 8), Comparison.GreaterThanOrEqualsTo)
                ? TypeSerializersTests.CustomTypeName2
                : TypeSerializersTests.CustomTypeName;

            var builder = ClusterBuilder()
                                 .AddContactPoint(TestCluster.InitialContactPoint)
                                 .WithTypeSerializers(new TypeSerializerDefinitions()
                                     .Define(new DummyCustomTypeSerializer(typeSerializerName)));
            using (var cluster = builder.Build())
            {
                var session = cluster.Connect(KeyspaceName);
                byte[] buffer =
                {
                    0x80, (byte)'i', 0, 4, 0, 0, 0, 1, 0
                };
                object[][] values =
                {
                    new object[] { Guid.NewGuid(), new DummyCustomType(buffer) }
                };
                var ps = session.Prepare(CustomInsertQuery);
                foreach (var item in values)
                {
                    var id = item[0];
                    var customValue = (DummyCustomType)item[1];
                    session.Execute(ps.Bind(id, item[1]));
                    var row = session.Execute(new SimpleStatement(CustomSelectQuery, id)).First();
                    Assert.AreEqual(row.GetValue<DummyCustomType>("value").Buffer, customValue.Buffer);
                }
            }
        }

        [Test, TestCassandraVersion(4, 0, Comparison.LessThan)]
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
            var elt = new FrameWriter(new MemoryStream(), new SerializerManager(ProtocolVersion.V1).GetCurrentSerializer());
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

        private static IEnumerable VectorTestCaseData()
        {
            var r = new Random();
            Action<object, object> defaultAssert = (expected, actual) => Assert.AreEqual(expected, actual);
            return new[]
            {
                    new TestCaseData("int", (Func<int>)(()=>r.Next()), defaultAssert),
                    new TestCaseData("bigint", (Func<long>)(()=>(long)r.NextDouble()), defaultAssert),
                    new TestCaseData("smallint", (Func<short>)(()=>(short)r.Next()), defaultAssert),
                    new TestCaseData("tinyint", (Func<sbyte>)(()=>(sbyte)r.Next()), defaultAssert),
                    new TestCaseData("varint", (Func<BigInteger>)(()=>new BigInteger((long)r.NextDouble())), defaultAssert),
                };
        }

        [Test, TestCassandraVersion(5, 0, Comparison.GreaterThanOrEqualsTo), TestCaseSource(nameof(VectorTestCaseData))]
        public void VectorSimpleStatementTest<T>(string cqlSubType, Func<T> elementGeneratorFn, Action<object, object> assertFn)
        {
            var tableName = "vectortest_" + cqlSubType.Replace("<", "A").Replace(">", "B").Replace(",", "C") + "isH";
            var ddl = $"CREATE TABLE IF NOT EXISTS {tableName} (i int PRIMARY KEY, j vector<{cqlSubType}, 3>)";
            Session.Execute(ddl);

            Action<Func<int, CqlVector<T>, SimpleStatement>> vectorSimpleStmtTestFn = simpleStmtFn =>
            {
                var vector = new CqlVector<T>(elementGeneratorFn(), elementGeneratorFn(), elementGeneratorFn());
                var i = new Random().Next();
                Session.Execute(simpleStmtFn(i, vector));
                var rs = Session.Execute($"SELECT * FROM {tableName} WHERE i = {i}");
                var rowList = rs.ToList();
                Assert.AreEqual(1, rowList.Count);
                var retrievedVector = rowList[0].GetValue<CqlVector<T>>("j");
                assertFn(vector, retrievedVector);
            };

            vectorSimpleStmtTestFn((i, v) => new SimpleStatement($"INSERT INTO {tableName} (i, j) VALUES (?, ?)", i, v));
            vectorSimpleStmtTestFn((i, v) => new SimpleStatement(new Dictionary<string, object> { { "index", i }, { "vector", v } }, $"INSERT INTO {tableName} (i, j) VALUES (:index, :vector)"));
        }
    }
}