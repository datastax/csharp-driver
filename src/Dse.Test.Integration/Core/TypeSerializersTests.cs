//
//  Copyright (C) DataStax, Inc.
//
//  Please see the license for details:
//  http://www.datastax.com/terms/datastax-dse-driver-license-terms
//

using System;
using System.Collections.Generic;
using System.Linq;
using System.Numerics;
using System.Text;
using Dse.Test.Integration.TestClusterManagement;
using Dse.Serialization;
using Dse.Test.Unit;
using Dse.Test.Unit.Extensions.Serializers;
using NUnit.Framework;

namespace Dse.Test.Integration.Core
{
    [Category("short"), Category("realcluster")]
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
                return new []
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
            var builder = Cluster.Builder()
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
            var builder = Cluster.Builder()
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
            var builder = Cluster.Builder()
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
            var builder = Cluster.Builder()
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
                    CollectionAssert.AreEqual(new BigDecimalSerializer().Serialize((ushort) session.BinaryProtocolVersion, v), statement.RoutingKey.RawRoutingKey);
                    session.Execute(statement);
                    var row = session.Execute(new SimpleStatement("SELECT * FROM tbl_decimal_key WHERE id = ?", v)).First();
                    Assert.AreEqual(row.GetValue<BigDecimal>("id"), v);
                }
            }
        }

        [Test]
        public void Should_Use_Custom_TypeSerializers()
        {
            var typeSerializerName = TestClusterManager.DseVersion <= new Version(6, 8)
                ? TypeSerializersTests.CustomTypeName
                : TypeSerializersTests.CustomTypeName2;

            var builder = Cluster.Builder()
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
                    var customValue = (DummyCustomType) item[1];
                    session.Execute(ps.Bind(id, item[1]));
                    var row = session.Execute(new SimpleStatement(CustomSelectQuery, id)).First();
                    Assert.AreEqual(row.GetValue<DummyCustomType>("value").Buffer, customValue.Buffer);
                }
            }
        }
    }
}
