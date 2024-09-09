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
using System.Globalization;
using System.IO;
using System.Linq;
using System.Net;
using System.Numerics;
using System.Threading;
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

        private static long vectorSchemaSetUp = 0;

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
            Action<object, object> defaultAssert = Assert.AreEqual;
            Action<object, object> listVectorAssert = (expected, actual) => CollectionAssert.AreEqual((IEnumerable)expected, (IEnumerable)actual);
            Action<object, object> setMapAssert = (expected, actual) => CollectionAssert.AreEquivalent((IEnumerable)expected, (IEnumerable)actual);
            var buf = new byte[128];
            return new[]
            {
                    new TestCaseData("int", (Func<int>)(()=>r.Next()), defaultAssert),
                    new TestCaseData("bigint", (Func<long>)(()=>(long)r.NextDouble()), defaultAssert),
                    new TestCaseData("smallint", (Func<short>)(()=>(short)r.Next()), defaultAssert),
                    new TestCaseData("tinyint", (Func<sbyte>)(()=>(sbyte)r.Next()), defaultAssert),
                    new TestCaseData("varint", (Func<BigInteger>)(()=>new BigInteger((long)r.NextDouble())), defaultAssert),

                    new TestCaseData("float", (Func<float>)(()=>(float)r.NextDouble()), defaultAssert),
                    new TestCaseData("double", (Func<double>)(()=>(double)r.NextDouble()), defaultAssert),
                    new TestCaseData("decimal", (Func<decimal>)(()=>(decimal)r.NextDouble()), defaultAssert),

                    new TestCaseData("ascii", (Func<string>)(()=>r.Next().ToString(CultureInfo.InvariantCulture)), defaultAssert),
                    new TestCaseData("text", (Func<string>)(()=>r.Next().ToString(CultureInfo.InvariantCulture)), defaultAssert),

                    new TestCaseData("date", (Func<LocalDate>)(()=>new LocalDate((uint)(r.Next()%100000))), defaultAssert),
                    new TestCaseData("time", (Func<LocalTime>)(()=>new LocalTime(r.Next())), defaultAssert),
                    new TestCaseData("timestamp", (Func<DateTimeOffset>)(()=>DateTimeOffset.FromUnixTimeMilliseconds(r.Next() % 10000000)), defaultAssert),

                    new TestCaseData("uuid", (Func<Guid>)(Guid.NewGuid), defaultAssert),
                    new TestCaseData("timeuuid", (Func<TimeUuid>)(TimeUuid.NewId), defaultAssert),
                     
                    new TestCaseData("boolean", (Func<bool>)(()=>r.Next()%2==0), defaultAssert),
                    new TestCaseData("duration", (Func<Duration>)(()=>Duration.FromTimeSpan(new TimeSpan(DateTime.UtcNow.Ticks))), defaultAssert),
                    new TestCaseData("inet", (Func<IPAddress>)(()=> IPAddress.Parse($"{(r.Next()%255) + 1}.{r.Next()%255}.{r.Next()%255}.{(r.Next()%255) + 1}")), defaultAssert),
                    new TestCaseData("blob", (Func<byte[]>)(()=>
                    {
                        r.NextBytes(buf);
                        return buf;
                    }), defaultAssert),

                    new TestCaseData("list<int>", (Func<int[]>)(()=>Enumerable.Range(0, r.Next()%200).Select(i => r.Next()).ToArray()), listVectorAssert),
                    new TestCaseData("list<int>", (Func<IEnumerable<int>>)(()=>Enumerable.Range(0, r.Next()%200).Select(i => r.Next()).ToList()), listVectorAssert),
                    new TestCaseData("list<int>", (Func<List<int>>)(()=>Enumerable.Range(0, r.Next()%200).Select(i => r.Next()).ToList()), listVectorAssert),
                    new TestCaseData("list<varint>", (Func<IEnumerable<BigInteger>>)(()=>Enumerable.Range(0, r.Next()%200).Select(i => new BigInteger((long)r.NextDouble())).ToList()), listVectorAssert),

                    new TestCaseData("set<int>", (Func<ISet<int>>)(()=>new HashSet<int>(Enumerable.Range(0, r.Next()%200).Distinct().Select(i => i))), setMapAssert),
                    new TestCaseData("set<int>", (Func<HashSet<int>>)(()=>new HashSet<int>(Enumerable.Range(0, r.Next()%200).Distinct().Select(i => i))), setMapAssert),

                    new TestCaseData("map<int,int>", (Func<IDictionary<int, int>>)(()=>Enumerable.Range(0, r.Next()%200).Distinct().ToDictionary(i => i, i => r.Next())), setMapAssert),
                    new TestCaseData("map<int,varint>", (Func<IDictionary<int, BigInteger>>)(()=>Enumerable.Range(0, r.Next()%200).Distinct().ToDictionary(i =>i, i => new BigInteger((long)r.NextDouble()))), setMapAssert),
                    new TestCaseData("map<varint,int>", (Func<IDictionary<BigInteger, int>>)(()=>Enumerable.Range(0, r.Next()%200).Distinct().ToDictionary(i => new BigInteger(i), i => r.Next())), setMapAssert),
                    new TestCaseData("map<varint,varint>", (Func<IDictionary<BigInteger, BigInteger>>)(()=>Enumerable.Range(0, r.Next()%200).Distinct().ToDictionary(i => new BigInteger(i), i => new BigInteger((long)r.NextDouble()))), setMapAssert),

                    new TestCaseData("vector<int,2>", (Func<CqlVector<int>>)(()=>new CqlVector<int>(Enumerable.Range(0, 2).Select(i => r.Next()).ToArray())), listVectorAssert),
                    new TestCaseData("vector<int,2>", (Func<CqlVector<int>>)(()=>new CqlVector<int>(r.Next(), r.Next())), listVectorAssert),
                    new TestCaseData("vector<varint,2>", (Func<CqlVector<BigInteger>>)(()=>new CqlVector<BigInteger>(Enumerable.Range(0, 2).Select(i => new BigInteger((long)r.NextDouble())).ToArray())), listVectorAssert),
                    
                    new TestCaseData("tuple<int,int>", (Func<Tuple<int,int>>)(()=>new Tuple<int, int>(r.Next(), r.Next())), defaultAssert),
                    new TestCaseData("tuple<int,varint>", (Func<Tuple<int,BigInteger>>)(()=>new Tuple<int, BigInteger>(r.Next(), new BigInteger((long)r.NextDouble()))), defaultAssert),
                    new TestCaseData("tuple<varint,int>", (Func<Tuple<BigInteger, int>>)(()=>new Tuple<BigInteger, int>(new BigInteger((long)r.NextDouble()), r.Next())), defaultAssert),
                    new TestCaseData("tuple<varint,varint>", (Func<Tuple<BigInteger, BigInteger>>)(()=>new Tuple<BigInteger, BigInteger>(new BigInteger((long)r.NextDouble()), new BigInteger((long)r.NextDouble()))), defaultAssert),

                    new TestCaseData("fixed_type", (Func<FixedType>)(()=>new FixedType { a = r.Next(), b = r.Next()}), defaultAssert),
                    new TestCaseData("mixed_type_one", (Func<MixedTypeOne>)(()=>new MixedTypeOne { a = r.Next(), b = (long)r.NextDouble()}), defaultAssert),
                    new TestCaseData("mixed_type_two", (Func<MixedTypeTwo>)(()=>new MixedTypeTwo { a = (long)r.NextDouble(), b = r.Next()}), defaultAssert),
                    new TestCaseData("var_type", (Func<VarType>)(()=>new VarType { a = (long)r.NextDouble(), b = (long)r.NextDouble()}), defaultAssert),
                    new TestCaseData("complex_vector_udt", (Func<ComplexVectorUdt>)(()=>new ComplexVectorUdt { a = new CqlVector<int>(r.Next(), r.Next(), r.Next()), b = new CqlVector<BigInteger>(r.Next(), r.Next(), r.Next())}), defaultAssert),
                };
        }

        [Test, TestBothServersVersion(5, 0, 6, 9), TestCaseSource(nameof(VectorTestCaseData))]
        public void VectorSimpleStatementTest<T>(string cqlSubType, Func<T> elementGeneratorFn, Action<object, object> assertFn)
        {
            SetupVectorUdtSchema();
            var baseName = "vectortest_" + cqlSubType.Replace("<", "A").Replace(">", "B").Replace(",", "C");
            var tableName = baseName + "isH";
            Session.Execute($"CREATE TABLE IF NOT EXISTS {tableName} (i int PRIMARY KEY, j vector<{cqlSubType}, 3>)");

            Action<Func<int, CqlVector<T>, SimpleStatement>> vectorSimpleStmtTestFn = simpleStmtFn =>
            {
                var vector = new CqlVector<T>(elementGeneratorFn(), elementGeneratorFn(), elementGeneratorFn());
                var i = new Random().Next();
                Session.Execute(simpleStmtFn(i, vector));
                var rs = Session.Execute($"SELECT * FROM {tableName} WHERE i = {i}");
                AssertSimpleVectorTest(vector, rs, assertFn);
            };

            vectorSimpleStmtTestFn((i, v) => new SimpleStatement($"INSERT INTO {tableName} (i, j) VALUES (?, ?)", i, v));
            vectorSimpleStmtTestFn((i, v) => new SimpleStatement(new Dictionary<string, object> { { "idx", i }, { "vec", v } }, $"INSERT INTO {tableName} (i, j) VALUES (:idx, :vec)"));
        }

        [Test, TestBothServersVersion(5, 0, 6, 9), TestCaseSource(nameof(VectorTestCaseData))]
        public void VectorSimpleStatementTestComplex<T>(string cqlSubType, Func<T> elementGeneratorFn, Action<object, object> assertFn)
        {
            SetupVectorUdtSchema();
            var baseName = "vectortest_" + cqlSubType.Replace("<", "A").Replace(">", "B").Replace(",", "C");
            var tableNameComplex = baseName + "_complex";
            Session.Execute($"CREATE TABLE IF NOT EXISTS {tableNameComplex} (i int PRIMARY KEY, k vector<vector<{cqlSubType}, 3>, 3>, l vector<list<vector<{cqlSubType}, 3>>, 3>)");

            Action<Func<int, List<CqlVector<T>>, SimpleStatement>> vectorSimpleStmtTestFn = simpleStmtFn =>
            {
                var vectorList = new List<CqlVector<T>>
                {
                    new CqlVector<T>(elementGeneratorFn(), elementGeneratorFn(), elementGeneratorFn()),
                    new CqlVector<T>(elementGeneratorFn(), elementGeneratorFn(), elementGeneratorFn()),
                    new CqlVector<T>(elementGeneratorFn(), elementGeneratorFn(), elementGeneratorFn()),
                };
                var i = new Random().Next();
                Session.Execute(simpleStmtFn(i, vectorList));
                var rs = Session.Execute($"SELECT * FROM {tableNameComplex} WHERE i = {i}");
                AssertComplexVectorTest(vectorList, rs, assertFn);
            };

            vectorSimpleStmtTestFn((i, v) => new SimpleStatement(
                $"INSERT INTO {tableNameComplex} (i, k, l) VALUES (?, ?, ?)", 
                i, 
                new CqlVector<CqlVector<T>>(v[0], v[1], v[2]), 
                new CqlVector<List<CqlVector<T>>>(new List<CqlVector<T>> { v[0] }, new List<CqlVector<T>> { v[1] }, new List<CqlVector<T>> { v[2] })));
            vectorSimpleStmtTestFn((i, v) => new SimpleStatement(
                new Dictionary<string, object>
                {
                    { "idx", i }, 
                    { "vec", new CqlVector<CqlVector<T>>(v[0], v[1], v[2]) }, 
                    { "vecc", new CqlVector<List<CqlVector<T>>>(new List<CqlVector<T>> { v[0] }, new List<CqlVector<T>> { v[1] }, new List<CqlVector<T>> { v[2] }) }
                }, 
                $"INSERT INTO {tableNameComplex} (i, k, l) VALUES (:idx, :vec, :vecc)"));
        }

        [Test, TestBothServersVersion(5, 0, 6, 9), TestCaseSource(nameof(VectorTestCaseData))]
        public void VectorPreparedStatementTest<T>(string cqlSubType, Func<T> elementGeneratorFn, Action<object, object> assertFn)
        {
            SetupVectorUdtSchema();
            var baseName = "vectortest_prep_" + cqlSubType.Replace("<", "A").Replace(">", "B").Replace(",", "C");
            var tableName = baseName + "isH";
            Session.Execute($"CREATE TABLE IF NOT EXISTS {tableName} (i int PRIMARY KEY, j vector<{cqlSubType}, 3>)");

            Action<string, Func<int, CqlVector<T>, PreparedStatement, BoundStatement>> vectorPreparedStmtTestFn = (cql, preparedStmtFn) =>
            {
                var vector = new CqlVector<T>(elementGeneratorFn(), elementGeneratorFn(), elementGeneratorFn());
                var i = new Random().Next();
                var ps = Session.Prepare(cql);
                Session.Execute(preparedStmtFn(i, vector, ps));
                ps = Session.Prepare($"SELECT * FROM {tableName} WHERE i = ?");
                var bs = ps.Bind(i);
                var rs = Session.Execute(bs);
                AssertSimpleVectorTest(vector, rs, assertFn);
            };

            vectorPreparedStmtTestFn($"INSERT INTO {tableName} (i, j) VALUES (?, ?)", (i, v, ps) => ps.Bind(i, v));
            vectorPreparedStmtTestFn($"INSERT INTO {tableName} (i, j) VALUES (:idx, :vec)", (i, v, ps) => ps.Bind(new { idx = i, vec = v }));
        }

        [Test, TestBothServersVersion(5, 0, 6, 9), TestCaseSource(nameof(VectorTestCaseData))]
        public void VectorPreparedStatementTestComplex<T>(string cqlSubType, Func<T> elementGeneratorFn, Action<object, object> assertFn)
        {
            SetupVectorUdtSchema();
            var baseName = "vectortest_prep_" + cqlSubType.Replace("<", "A").Replace(">", "B").Replace(",", "C");
            var tableNameComplex = baseName + "_complex";
            Session.Execute($"CREATE TABLE IF NOT EXISTS {tableNameComplex} (i int PRIMARY KEY, k vector<vector<{cqlSubType}, 3>, 3>, l vector<list<vector<{cqlSubType}, 3>>, 3>)");

            Action<string, Func<int, List<CqlVector<T>>, PreparedStatement, BoundStatement>> vectorPreparedStmtTestFn = (cql, preparedStmtFn) =>
            {
                var vectorList = new List<CqlVector<T>>
                {
                    new CqlVector<T>(elementGeneratorFn(), elementGeneratorFn(), elementGeneratorFn()),
                    new CqlVector<T>(elementGeneratorFn(), elementGeneratorFn(), elementGeneratorFn()),
                    new CqlVector<T>(elementGeneratorFn(), elementGeneratorFn(), elementGeneratorFn()),
                };
                var i = new Random().Next();
                var ps = Session.Prepare(cql);
                Session.Execute(preparedStmtFn(i, vectorList, ps));
                ps = Session.Prepare($"SELECT * FROM {tableNameComplex} WHERE i = ?");
                var bs = ps.Bind(i);
                var rs = Session.Execute(bs);
                AssertComplexVectorTest(vectorList, rs, assertFn);
            };

            vectorPreparedStmtTestFn(
                $"INSERT INTO {tableNameComplex} (i, k, l) VALUES (?, ?, ?)",
                (i, v, ps) =>
                    ps.Bind(
                        i,
                        new CqlVector<CqlVector<T>>(v[0], v[1], v[2]),
                        new CqlVector<List<CqlVector<T>>>(new List<CqlVector<T>> { v[0] }, new List<CqlVector<T>> { v[1] }, new List<CqlVector<T>> { v[2] })));
            vectorPreparedStmtTestFn(
                $"INSERT INTO {tableNameComplex} (i, k, l) VALUES (:idx, :vec, :vecc)",
                (i, v, ps) =>
                    ps.Bind(
                        new
                        {
                            idx = i,
                            vec = new CqlVector<CqlVector<T>>(v[0], v[1], v[2]),
                            vecc = new CqlVector<List<CqlVector<T>>>(new List<CqlVector<T>> { v[0] }, new List<CqlVector<T>> { v[1] }, new List<CqlVector<T>> { v[2] })
                        }
                ));
        }

        [Test, TestBothServersVersion(5, 0, 6, 9), TestCaseSource(nameof(VectorTestCaseData))]
        public void VectorTestCollectionConversion<T>(string cqlSubType, Func<T> elementGeneratorFn, Action<object, object> assertFn)
        {
            SetupVectorUdtSchema();
            var baseName = "vectortest_conv_" + cqlSubType.Replace("<", "A").Replace(">", "B").Replace(",", "C");
            var tableName = baseName + "isH";
            Session.Execute($"CREATE TABLE IF NOT EXISTS {tableName} (i int PRIMARY KEY, j vector<{cqlSubType}, 3>)");

            Action<string, Func<int, CqlVector<T>, PreparedStatement, BoundStatement>> vectorPreparedStmtTestFn = (cql, preparedStmtFn) =>
            {
                var vector = new CqlVector<T>(elementGeneratorFn(), elementGeneratorFn(), elementGeneratorFn());
                var i = new Random().Next();
                var ps = Session.Prepare(cql);
                Session.Execute(preparedStmtFn(i, vector, ps));
                ps = Session.Prepare($"SELECT * FROM {tableName} WHERE i = ?");
                var bs = ps.Bind(i);
                var rs = Session.Execute(bs);
                var rowList = rs.ToList();

                Assert.AreEqual(1, rowList.Count);
                var retrievedVector1 = rowList[0].GetValue<IEnumerable<T>>("j");
                AssertSimpleVectorEquals(vector, retrievedVector1, assertFn);
                var retrievedVector2 = rowList[0].GetValue<T[]>("j");
                AssertSimpleVectorEquals(vector, retrievedVector2, assertFn);
                var retrievedVector3 = rowList[0].GetValue<List<T>>("j");
                AssertSimpleVectorEquals(vector, retrievedVector3, assertFn);
                var retrievedVector4 = rowList[0].GetValue<IList<T>>("j");
                AssertSimpleVectorEquals(vector, retrievedVector4, assertFn);
                var retrievedVector5 = rowList[0].GetValue<ICollection<T>>("j");
                AssertSimpleVectorEquals(vector, retrievedVector5, assertFn);
            };

            vectorPreparedStmtTestFn($"INSERT INTO {tableName} (i, j) VALUES (?, ?)", (i, v, ps) => ps.Bind(i, v));
            vectorPreparedStmtTestFn($"INSERT INTO {tableName} (i, j) VALUES (:idx, :vec)", (i, v, ps) => ps.Bind(new { idx = i, vec = v }));
        }

        [Test, TestBothServersVersion(5, 0, 6, 9), TestCaseSource(nameof(VectorTestCaseData))]
        public void VectorTestCollectionConversionComplex<T>(string cqlSubType, Func<T> elementGeneratorFn, Action<object, object> assertFn)
        {
            SetupVectorUdtSchema();
            var baseName = "vectortest_conv_" + cqlSubType.Replace("<", "A").Replace(">", "B").Replace(",", "C");
            var tableNameComplex = baseName + "_complex";
            Session.Execute($"CREATE TABLE IF NOT EXISTS {tableNameComplex} (i int PRIMARY KEY, k vector<vector<{cqlSubType}, 3>, 3>, l vector<list<vector<{cqlSubType}, 3>>, 3>)");

            Action<string, Func<int, List<CqlVector<T>>, PreparedStatement, BoundStatement>> vectorPreparedStmtTestFn = (cql, preparedStmtFn) =>
            {
                var vectorList = new List<CqlVector<T>>
                {
                    new CqlVector<T>(elementGeneratorFn(), elementGeneratorFn(), elementGeneratorFn()),
                    new CqlVector<T>(elementGeneratorFn(), elementGeneratorFn(), elementGeneratorFn()),
                    new CqlVector<T>(elementGeneratorFn(), elementGeneratorFn(), elementGeneratorFn()),
                };
                var i = new Random().Next();
                var ps = Session.Prepare(cql);
                Session.Execute(preparedStmtFn(i, vectorList, ps));
                ps = Session.Prepare($"SELECT * FROM {tableNameComplex} WHERE i = ?");
                var bs = ps.Bind(i);
                var rs = Session.Execute(bs);
                var rowList = rs.ToList();

                Assert.AreEqual(1, rowList.Count);
                var retrievedVector11 = rowList[0].GetValue<IEnumerable<IEnumerable<T>>>("k");
                var retrievedVector12 = rowList[0].GetValue<IEnumerable<IEnumerable<IEnumerable<T>>>>("l");
                AssertComplexVectorEquals(vectorList, retrievedVector11, retrievedVector12, assertFn);

                var retrievedVector21 = rowList[0].GetValue<T[][]>("k");
                var retrievedVector22 = rowList[0].GetValue<T[][][]>("l");
                AssertComplexVectorEquals(vectorList, retrievedVector21, retrievedVector22, assertFn);

                var retrievedVector31 = rowList[0].GetValue<List<List<T>>>("k");
                var retrievedVector32 = rowList[0].GetValue<List<List<List<T>>>>("l");
                AssertComplexVectorEquals(vectorList, retrievedVector31, retrievedVector32, assertFn);

                var retrievedVector41 = rowList[0].GetValue<ICollection<ICollection<T>>>("k");
                var retrievedVector42 = rowList[0].GetValue<ICollection<ICollection<ICollection<T>>>>("l");
                AssertComplexVectorEquals(vectorList, retrievedVector41, retrievedVector42, assertFn);
            };

            vectorPreparedStmtTestFn(
                $"INSERT INTO {tableNameComplex} (i, k, l) VALUES (?, ?, ?)",
                (i, v, ps) =>
                    ps.Bind(
                        i,
                        new CqlVector<CqlVector<T>>(v[0], v[1], v[2]),
                        new CqlVector<List<CqlVector<T>>>(new List<CqlVector<T>> { v[0] }, new List<CqlVector<T>> { v[1] }, new List<CqlVector<T>> { v[2] })));
            vectorPreparedStmtTestFn(
                $"INSERT INTO {tableNameComplex} (i, k, l) VALUES (:idx, :vec, :vecc)",
                (i, v, ps) =>
                    ps.Bind(
                        new
                        {
                            idx = i,
                            vec = new CqlVector<CqlVector<T>>(v[0], v[1], v[2]),
                            vecc = new CqlVector<List<CqlVector<T>>>(new List<CqlVector<T>> { v[0] }, new List<CqlVector<T>> { v[1] }, new List<CqlVector<T>> { v[2] })
                        }
                ));
        }

        private void AssertSimpleVectorTest<T>(CqlVector<T> expected, RowSet rs, Action<object, object> assertFn)
        {
            var rowList = rs.ToList();
            Assert.AreEqual(1, rowList.Count);
            var retrievedVector = rowList[0].GetValue<CqlVector<T>>("j");
            Assert.AreEqual(3, retrievedVector.Count);
            for (var idx = 0; idx < retrievedVector.Count; idx++)
            {
                assertFn(expected[idx], retrievedVector[idx]);
            }
        }

        private void AssertSimpleVectorEquals<T>(CqlVector<T> expected, IEnumerable<T> actual, Action<object, object> assertFn)
        {
            var list = actual.ToList();
            Assert.AreEqual(3, list.Count);
            for (var idx = 0; idx < list.Count; idx++)
            {
                assertFn(expected[idx], list[idx]);
            }
        }

        private void AssertComplexVectorTest<T>(List<CqlVector<T>> vectorList, RowSet rs, Action<object, object> assertFn)
        {
            var rowList = rs.ToList();
            Assert.AreEqual(1, rowList.Count);

            var retrievedVector1 = rowList[0].GetValue<CqlVector<CqlVector<T>>>("k");
            Assert.AreEqual(3, retrievedVector1.Count);
            for (var idx = 0; idx < 3; idx++)
            {
                Assert.AreEqual(3, retrievedVector1[idx].Count);
                for (var idxj = 0; idxj < vectorList[idx].Count; idxj++)
                {
                    assertFn(vectorList[idx][idxj], retrievedVector1[idx][idxj]);
                }
            }
            var retrievedVector2 = rowList[0].GetValue<CqlVector<List<CqlVector<T>>>>("l");
            Assert.AreEqual(3, retrievedVector2.Count);
            for (var idx = 0; idx < 3; idx++)
            {
                Assert.AreEqual(1, retrievedVector2[idx].Count);
                Assert.AreEqual(3, retrievedVector2[idx][0].Count);
                for (var idxj = 0; idxj < vectorList[idx].Count; idxj++)
                {
                    assertFn(vectorList[idx][idxj], retrievedVector2[idx][0][idxj]);
                }
            }
        }

        private void AssertComplexVectorEquals<T>(
            List<CqlVector<T>> vectorList, 
            IEnumerable<IEnumerable<T>> actual1, 
            IEnumerable<IEnumerable<IEnumerable<T>>> actual2, 
            Action<object, object> assertFn)
        {
            var retrievedVector1 = actual1.ToList();
            Assert.AreEqual(3, retrievedVector1.Count);
            for (var idx = 0; idx < 3; idx++)
            {
                var elem = retrievedVector1[idx].ToList();
                Assert.AreEqual(3, elem.Count);
                for (var idxj = 0; idxj < vectorList[idx].Count; idxj++)
                {
                    assertFn(vectorList[idx][idxj], elem[idxj]);
                }
            }

            var retrievedVector2 = actual2.ToList();
            Assert.AreEqual(3, retrievedVector2.Count);
            for (var idx = 0; idx < 3; idx++)
            {
                var elem = retrievedVector2[idx].ToList();
                Assert.AreEqual(1, elem.Count);
                var subElem = elem[0].ToList();
                Assert.AreEqual(3, subElem.Count);
                for (var idxj = 0; idxj < vectorList[idx].Count; idxj++)
                {
                    assertFn(vectorList[idx][idxj], subElem[idxj]);
                }
            }
        }

        private void SetupVectorUdtSchema()
        {
            if (Interlocked.CompareExchange(ref vectorSchemaSetUp, 1, 0) == 0)
            {
                Session.Execute("create type if not exists fixed_type (a int, b int)");
                Session.Execute("create type if not exists mixed_type_one (a int, b varint)");
                Session.Execute("create type if not exists mixed_type_two (a varint, b int)");
                Session.Execute("create type if not exists var_type (a varint, b varint)");
                Session.Execute("create type if not exists complex_vector_udt (a vector<int, 3>, b vector<varint, 3>)");
            }
            Session.UserDefinedTypes.Define(
                UdtMap.For<FixedType>("fixed_type"),
                UdtMap.For<MixedTypeOne>("mixed_type_one"),
                UdtMap.For<MixedTypeTwo>("mixed_type_two"),
                UdtMap.For<VarType>("var_type"),
                UdtMap.For<ComplexVectorUdt>("complex_vector_udt"));
        }

        public class FixedType
        {
            protected bool Equals(FixedType other)
            {
                return a == other.a && b == other.b;
            }

            public override bool Equals(object obj)
            {
                if (obj is null) return false;
                if (ReferenceEquals(this, obj)) return true;
                if (obj.GetType() != GetType()) return false;
                return Equals((FixedType)obj);
            }

            public override int GetHashCode()
            {
                unchecked
                {
                    return (a * 397) ^ b;
                }
            }

            public int a { get; set; }

            public int b { get; set; }
        }

        public class MixedTypeOne
        {
            protected bool Equals(MixedTypeOne other)
            {
                return a == other.a && b.Equals(other.b);
            }

            public override bool Equals(object obj)
            {
                if (obj is null) return false;
                if (ReferenceEquals(this, obj)) return true;
                if (obj.GetType() != GetType()) return false;
                return Equals((MixedTypeOne)obj);
            }

            public override int GetHashCode()
            {
                unchecked
                {
                    return (a * 397) ^ b.GetHashCode();
                }
            }

            public int a { get; set; }

            public BigInteger b { get; set; }
        }

        public class MixedTypeTwo
        {
            protected bool Equals(MixedTypeTwo other)
            {
                return a.Equals(other.a) && b == other.b;
            }

            public override bool Equals(object obj)
            {
                if (obj is null) return false;
                if (ReferenceEquals(this, obj)) return true;
                if (obj.GetType() != GetType()) return false;
                return Equals((MixedTypeTwo)obj);
            }

            public override int GetHashCode()
            {
                unchecked
                {
                    return (a.GetHashCode() * 397) ^ b;
                }
            }

            public BigInteger a { get; set; }

            public int b { get; set; }
        }

        public class VarType
        {
            protected bool Equals(VarType other)
            {
                return a.Equals(other.a) && b.Equals(other.b);
            }

            public override bool Equals(object obj)
            {
                if (obj is null) return false;
                if (ReferenceEquals(this, obj)) return true;
                if (obj.GetType() != GetType()) return false;
                return Equals((VarType)obj);
            }

            public override int GetHashCode()
            {
                unchecked
                {
                    return (a.GetHashCode() * 397) ^ b.GetHashCode();
                }
            }

            public BigInteger a { get; set; }

            public BigInteger b { get; set; }
        }

        public class ComplexVectorUdt
        {
            protected bool Equals(ComplexVectorUdt other)
            {
                return Equals(a, other.a) && Equals(b, other.b);
            }

            public override bool Equals(object obj)
            {
                if (obj is null) return false;
                if (ReferenceEquals(this, obj)) return true;
                if (obj.GetType() != GetType()) return false;
                return Equals((ComplexVectorUdt)obj);
            }

            public override int GetHashCode()
            {
                unchecked
                {
                    return ((a != null ? a.GetHashCode() : 0) * 397) ^ (b != null ? b.GetHashCode() : 0);
                }
            }

            public CqlVector<int> a { get; set; }

            public CqlVector<BigInteger> b { get; set; }
        }
    }
}