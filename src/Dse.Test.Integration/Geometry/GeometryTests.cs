//
//  Copyright (C) 2016 DataStax, Inc.
//
//  Please see the license for details:
//  http://www.datastax.com/terms/datastax-dse-driver-license-terms
//
using System;
using System.Collections;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Text;
using System.Threading;
using Cassandra;
using Cassandra.IntegrationTests.TestBase;
using NUnit.Framework;
using Dse.Geometry;
using Dse.Test.Integration.ClusterManagement;

namespace Dse.Test.Integration.Geometry
{
    [TestDseVersion(5, 0)]
    public abstract class GeometryTests<T> : BaseIntegrationTest
    {
        protected abstract T[] Values { get; }
        protected abstract string TypeName { get; }

        protected IDseCluster Cluster;
        protected IDseSession Session;
        protected readonly string Keyspace;

        private const string CreateKeyspaceQuery =
            "CREATE KEYSPACE {0} WITH replication = {{'class': 'SimpleStrategy', 'replication_factor' : 1}} and durable_writes = false";

        private string[] _queries;
        private const string InsertGeoQuery = "INSERT INTO geotable1 (id, value) VALUES (?, ?)";
        public const string SelectGeoQuery = "SELECT * FROM geotable1 WHERE id = ?";
        private const string SelectKeyedQuery = "SELECT * FROM keyed WHERE id = ?";

        protected GeometryTests()
        {
            Keyspace = "ks_" + GetType().Name.ToLowerInvariant();
        }

        [OneTimeSetUp]
        public void OneTimeSetUp()
        {
            if (Values == null || Values.Length == 0)
            {
                throw new InconclusiveException("You must specify the values to test");
            }
            CcmHelper.Start(1);
            Cluster = DseCluster.Builder().AddContactPoint(CcmHelper.InitialContactPoint).Build();
            Trace.TraceInformation("Waiting additional time for test Cluster to be ready");
            Thread.Sleep(15000);
            Session = Cluster.Connect();
            Session.Execute(string.Format(CreateKeyspaceQuery, Keyspace));
            Session.Execute(string.Format("USE {0}", Keyspace));
            _queries = new[]
            {
                string.Format("CREATE TABLE geotable1 (id text, value '{0}', PRIMARY KEY (id))", TypeName),
                string.Format("CREATE TABLE keyed (id '{0}', value text, PRIMARY KEY (id))", TypeName),
                string.Format("INSERT INTO keyed (id, value) VALUES ('{0}', 'hello')", Values[0]),
                string.Format("CREATE TYPE geo_udt (f text, v '{0}')", TypeName),
                "CREATE TABLE tbl_udts (id uuid PRIMARY KEY, value frozen<geo_udt>)",
                string.Format("CREATE TABLE tbl_tuple (id uuid PRIMARY KEY, value tuple<int, '{0}'>)", TypeName),
                string.Format("CREATE TABLE tbl_list (id uuid PRIMARY KEY, value list<'{0}'>)", TypeName),
                string.Format("CREATE TABLE tbl_set (id uuid PRIMARY KEY, value set<'{0}'>)", TypeName),
                string.Format("CREATE TABLE tbl_map (id uuid PRIMARY KEY, value map<text, '{0}'>)", TypeName)
            };
            foreach (var query in _queries)
            {
                Session.Execute(query);
            }
        }

        [OneTimeTearDown]
        public void OneTimeTearDown()
        {
            if (Cluster != null)
            {
                Cluster.Dispose();
            }
            CcmHelper.Remove();
        }

        private void SerializeDeserializeTest(bool prepared)
        {
            PreparedStatement insertPs = null;
            PreparedStatement selectPs = null;
            var prefix = "simple-";
            if (prepared)
            {
                insertPs = Session.Prepare(InsertGeoQuery);
                selectPs = Session.Prepare(SelectGeoQuery);
                prefix = "prepared-";
            }
            foreach (var value in Values)
            {
                var id = prefix + value;
                var statement = prepared ? 
                    (IStatement) insertPs.Bind(id, value) :
                    new SimpleStatement(InsertGeoQuery, id, value);
                Session.Execute(statement);

                statement = prepared ?
                    (IStatement)selectPs.Bind(id) :
                    new SimpleStatement(SelectGeoQuery, id);
                var row = Session.Execute(statement).FirstOrDefault();
                Assert.NotNull(row, "Row with id '{0}' not found", id);
                Assert.AreEqual(value, row.GetValue<object>("value"));
            }
        }

        [Test]
        public void Serialize_Deserialize_Prepared_Test()
        {
            SerializeDeserializeTest(true);
        }

        [Test]
        public void Serialize_Deserialize_Simple_Test()
        {
            SerializeDeserializeTest(false);
        }

        [Test]
        public void Retrieve_With_Partition_Key_Test()
        {
            var ps = Session.Prepare(SelectKeyedQuery);
            var statement = ps.Bind(Values[0]);
            var row = Session.Execute(statement).FirstOrDefault();
            Assert.NotNull(row);
            Assert.AreEqual("hello", row.GetValue<string>("value"));
            Assert.NotNull(statement.RoutingKey);
            Assert.NotNull(statement.RoutingKey.RawRoutingKey);
        }

        [TestCase(true)]
        [TestCase(false)]
        public void Serialize_Deserialize_Tuple_Test(bool prepared)
        {
            SerializeDeserializeCollectionsTest(prepared, "tbl_tuple", Tuple.Create(1, Values[0]));
        }

        [TestCase(true)]
        [TestCase(false)]
        public void Serialize_Deserialize_List_Test(bool prepared)
        {
            SerializeDeserializeCollectionsTest(prepared, "tbl_list", Values);
        }

        [TestCase(true)]
        [TestCase(false)]
        public void Serialize_Deserialize_Set_Test(bool prepared)
        {
            SerializeDeserializeCollectionsTest(prepared, "tbl_set", new[] { Values[0] });
        }

        [TestCase(true)]
        [TestCase(false)]
        public void Serialize_Deserialize_Map_Test(bool prepared)
        {
            SerializeDeserializeCollectionsTest(prepared, "tbl_map", 
                new Dictionary<string, T>
                {
                    {"a1", Values[0] }
                });
        }

        private void SerializeDeserializeCollectionsTest(bool prepared, string table, object value)
        {
            PreparedStatement insertPs = null;
            PreparedStatement selectPs = null;
            var insertQuery = string.Format("INSERT INTO {0} (id, value) VALUES (?, ?)", table);
            var selectQuery = string.Format("SELECT * FROM {0} where id = ?", table);
            if (prepared)
            {
                insertPs = Session.Prepare(insertQuery);
                selectPs = Session.Prepare(selectQuery);
            }
            var id = Guid.NewGuid();
            var statement = prepared ?
                (IStatement)insertPs.Bind(id, value) :
                new SimpleStatement(insertQuery, id, value);
            Session.Execute(statement);
            statement = prepared ?
                (IStatement)selectPs.Bind(id) :
                new SimpleStatement(selectQuery, id);
            var row = Session.Execute(statement).FirstOrDefault();
            Assert.NotNull(row, "Row for value {0} not found", value);
            if (value is IEnumerable)
            {
                CollectionAssert.AreEqual((IEnumerable) value, row.GetValue<IEnumerable>("value"));
            }
            else
            {
                Assert.AreEqual(value, row.GetValue<object>("value"));
            }
        }
    }
}
