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

using System.Diagnostics;
using Cassandra.Data.Linq;
using Cassandra.IntegrationTests.Linq.Structures;
using Cassandra.IntegrationTests.TestBase;
using Cassandra.IntegrationTests.TestClusterManagement;
using NUnit.Framework;
using System;
using System.Collections.Generic;
using System.Linq;
using Cassandra.Mapping;
using Cassandra.Mapping.Statements;

namespace Cassandra.IntegrationTests.Core
{
    [Category("long")]
    public class ConsistencyTests : TestGlobals
    {
        private ISession _session;
        private string _ksName;
        private Table<ManyDataTypesEntity> _table;
        private string _defaultSelectStatement = "SELECT * FROM \"" + typeof(ManyDataTypesEntity).Name + "\"";
        private string _preparedInsertStatementAsString =
            "INSERT INTO \"" + typeof(ManyDataTypesEntity).Name + "\" (\"StringType\", \"GuidType\", \"DateTimeType\", \"DateTimeOffsetType\", \"BooleanType\", " +
            "\"DecimalType\", \"DoubleType\", \"FloatType\", \"NullableIntType\", \"IntType\", \"Int64Type\", " +
            "\"DictionaryStringLongType\", \"DictionaryStringStringType\", \"ListOfGuidsType\", \"ListOfStringsType\") VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";
        private string _simpleStatementInsertFormat =
            "INSERT INTO \"" + typeof(ManyDataTypesEntity).Name + "\" (\"StringType\", \"GuidType\", \"BooleanType\", " +
            "\"DecimalType\", \"DoubleType\", \"FloatType\", \"IntType\", \"Int64Type\") VALUES ('{0}', {1}, {2}, {3}, {4}, {5}, {6}, {7})";

        private List<ManyDataTypesEntity> _defaultPocoList;
        private int _defaultNodeCountOne = 1;
        private PreparedStatement _preparedStatement;

        [TearDown]
        public void TeardownTest()
        {
            TestUtils.TryToDeleteKeyspace(_session, _ksName);
        }

        private ITestCluster SetupSessionAndCluster(int nodes, Dictionary<string, string> replication = null)
        {
            ITestCluster testCluster = TestClusterManager.GetTestCluster(nodes);
            _session = testCluster.Session;
            _ksName = TestUtils.GetUniqueKeyspaceName();
            _session.CreateKeyspace(_ksName, replication);
            TestUtils.WaitForSchemaAgreement(_session.Cluster);
            _session.ChangeKeyspace(_ksName);
            _table = new Table<ManyDataTypesEntity>(_session, new MappingConfiguration());
            _table.Create();
            _defaultPocoList = ManyDataTypesEntity.GetDefaultAllDataTypesList();
            _preparedStatement = _session.Prepare(_preparedInsertStatementAsString);
            foreach (var manyDataTypesEntity in _defaultPocoList)
                _session.Execute(GetBoundInsertStatementBasedOnEntity(manyDataTypesEntity));

            return testCluster;
        }

        private BoundStatement GetBoundInsertStatementBasedOnEntity(ManyDataTypesEntity entity)
        {
            BoundStatement boundStatement = _preparedStatement.Bind(ConvertEntityToObjectArray(entity));
            return boundStatement;
        }

        private string GetSimpleStatementInsertString(ManyDataTypesEntity entity)
        {
            string strForSimpleStatement = string.Format(_simpleStatementInsertFormat, 
                new object[] {
                entity.StringType, entity.GuidType, entity.BooleanType, entity.DecimalType, 
                entity.DoubleType, entity.FloatType, entity.IntType, entity.Int64Type
                });
            return strForSimpleStatement;
        }

        //////////////////////////////////////////////////
        /// Begin SimpleStatement Tests
        //////////////////////////////////////////////////

        [Test]
        public void Consistency_SimpleStatement_LocalSerial_Insert_Success()
        {
            SetupSessionAndCluster(_defaultNodeCountOne);

            string simpleStatementStr = GetSimpleStatementInsertString(ManyDataTypesEntity.GetRandomInstance());
            SimpleStatement simpleStatement = new SimpleStatement(simpleStatementStr);
            simpleStatement = (SimpleStatement)simpleStatement.SetConsistencyLevel(ConsistencyLevel.Quorum).SetSerialConsistencyLevel(ConsistencyLevel.LocalSerial);
            var result = _session.Execute(simpleStatement);
            Assert.AreEqual(ConsistencyLevel.Quorum, result.Info.AchievedConsistency);

            var selectResult = _session.Execute(_defaultSelectStatement);
            Assert.AreEqual(_defaultPocoList.Count + 1, selectResult.GetRows().ToList().Count);
        }

        [Test]
        public void Consistency_SimpleStatement_LocalSerial_Insert_Fail()
        {
            SetupSessionAndCluster(_defaultNodeCountOne);
            Assert.Throws<InvalidQueryException>(() => DoSimpleStatementInsertTest(ConsistencyLevel.LocalSerial));
        }

        [Test]
        public void Consistency_SimpleStatement_LocalSerial_Select_Success()
        {
            SetupSessionAndCluster(_defaultNodeCountOne);
            // Read consistency specified and write consistency specified
            SimpleStatement statement = (SimpleStatement)new SimpleStatement(_defaultSelectStatement).SetConsistencyLevel(ConsistencyLevel.Quorum).SetSerialConsistencyLevel(ConsistencyLevel.LocalSerial);
            var result = _session.Execute(statement);
            Assert.AreEqual(ConsistencyLevel.Quorum, result.Info.AchievedConsistency);
            Assert.AreEqual(_defaultPocoList.Count, result.GetRows().ToList().Count);

        }

        [Test]
        public void Consistency_SimpleStatement_LocalSerial_Select_Fail()
        {
            SetupSessionAndCluster(_defaultNodeCountOne);
            Assert.DoesNotThrow(() => DoSimpleStatementSelectTest(ConsistencyLevel.LocalSerial));
        }

        [Test]
        public void Consistency_SimpleStatement_Serial_Insert_Success()
        {
            SetupSessionAndCluster(_defaultNodeCountOne);
            string simpleStatementStr = GetSimpleStatementInsertString(ManyDataTypesEntity.GetRandomInstance());
            SimpleStatement simpleStatement = new SimpleStatement(simpleStatementStr);
            simpleStatement = (SimpleStatement)simpleStatement.SetConsistencyLevel(ConsistencyLevel.Quorum).SetSerialConsistencyLevel(ConsistencyLevel.Serial);
            var result = _session.Execute(simpleStatement);
            Assert.AreEqual(ConsistencyLevel.Quorum, result.Info.AchievedConsistency);

            var selectResult = _session.Execute(_defaultSelectStatement);
            Assert.AreEqual(_defaultPocoList.Count + 1, selectResult.GetRows().ToList().Count);
        }

        [Test]
        public void Consistency_SimpleStatement_Serial_Insert_Fail()
        {
            SetupSessionAndCluster(_defaultNodeCountOne);
            Assert.Throws<InvalidQueryException>(() => DoSimpleStatementInsertTest(ConsistencyLevel.Serial));
        }

        [Test]
        public void Consistency_SimpleStatement_Serial_Select_Success()
        {
            SetupSessionAndCluster(_defaultNodeCountOne);
            SetupSessionAndCluster(_defaultNodeCountOne);
            // Read consistency specified and write consistency specified
            SimpleStatement statement = (SimpleStatement)new SimpleStatement(_defaultSelectStatement).SetConsistencyLevel(ConsistencyLevel.Quorum).SetSerialConsistencyLevel(ConsistencyLevel.Serial);
            var result = _session.Execute(statement);
            Assert.AreEqual(ConsistencyLevel.Quorum, result.Info.AchievedConsistency);
            Assert.AreEqual(_defaultPocoList.Count, result.GetRows().ToList().Count);

        }

        [Test]
        public void Consistency_SimpleStatement_Serial_Select_Does_Not_Throw()
        {
            SetupSessionAndCluster(_defaultNodeCountOne);
            Assert.DoesNotThrow(() => DoSimpleStatementSelectTest(ConsistencyLevel.Serial));
        }


        [Test]
        public void Consistency_SimpleStatement_LocalOne_Select()
        {
            SetupSessionAndCluster(_defaultNodeCountOne);
            DoSimpleStatementSelectTest(ConsistencyLevel.LocalOne);
        }

        [Test]
        public void Consistency_SimpleStatement_LocalOne_Insert()
        {
            SetupSessionAndCluster(_defaultNodeCountOne);
            DoSimpleStatementInsertTest(ConsistencyLevel.LocalOne);
        }

        [Test]
        public void Consistency_SimpleStatement_Quorum_Select()
        {
            SetupSessionAndCluster(_defaultNodeCountOne);
            DoSimpleStatementSelectTest(ConsistencyLevel.Quorum);
        }

        [Test]
        public void Consistency_SimpleStatement_Quorum_Insert()
        {
            SetupSessionAndCluster(_defaultNodeCountOne);
            DoSimpleStatementInsertTest(ConsistencyLevel.Quorum);
        }

        [Test]
        public void Consistency_SimpleStatement_All_Select()
        {
            SetupSessionAndCluster(_defaultNodeCountOne);
            DoSimpleStatementSelectTest(ConsistencyLevel.All);
        }

        [Test]
        public void Consistency_SimpleStatement_All_Insert()
        {
            SetupSessionAndCluster(_defaultNodeCountOne);
            DoSimpleStatementInsertTest(ConsistencyLevel.All);
        }

        [Test]
        public void Consistency_SimpleStatement_Any_Select()
        {
            SetupSessionAndCluster(_defaultNodeCountOne);
            var ex = Assert.Throws<InvalidQueryException>(() => DoSimpleStatementSelectTest(ConsistencyLevel.Any));
            Assert.AreEqual("ANY ConsistencyLevel is only supported for writes", ex.Message);
        }

        [Test]
        public void Consistency_SimpleStatement_Any_Insert()
        {
            SetupSessionAndCluster(_defaultNodeCountOne);
            DoSimpleStatementInsertTest(ConsistencyLevel.Any);
        }

        [Test]
        public void Consistency_SimpleStatement_EachQuorum_SelectFails()
        {
            SetupSessionAndCluster(_defaultNodeCountOne);
            var ex = Assert.Throws<InvalidQueryException>(() => DoSimpleStatementSelectTest(ConsistencyLevel.EachQuorum));
            Assert.AreEqual("EACH_QUORUM ConsistencyLevel is only supported for writes", ex.Message);
        }

        [Test]
        public void Consistency_SimpleStatement_LocalQuorum_Select()
        {
            SetupSessionAndCluster(_defaultNodeCountOne);
            DoSimpleStatementSelectTest(ConsistencyLevel.LocalQuorum);
        }

        [Test]
        public void Consistency_SimpleStatement_EachQuorum_InsertSucceeds()
        {
            SetupSessionAndCluster(_defaultNodeCountOne);
            DoSimpleStatementInsertTest(ConsistencyLevel.EachQuorum);
        }

        [Test]
        public void Consistency_SimpleStatement_LocalQuorum_Insert()
        {
            SetupSessionAndCluster(_defaultNodeCountOne);
            DoSimpleStatementInsertTest(ConsistencyLevel.LocalQuorum);
        }

        [Test]
        public void Consistency_SimpleStatement_One_Select()
        {
            SetupSessionAndCluster(_defaultNodeCountOne);
            DoSimpleStatementSelectTest(ConsistencyLevel.One);
        }

        [Test]
        public void Consistency_SimpleStatement_One_Insert()
        {
            SetupSessionAndCluster(_defaultNodeCountOne);
            DoSimpleStatementInsertTest(ConsistencyLevel.One);
        }

        [Test]
        public void Consistency_SimpleStatement_Three_Select_NotEnoughReplicas()
        {
            SetupSessionAndCluster(_defaultNodeCountOne);
            var ex = Assert.Throws<UnavailableException>(() => DoSimpleStatementInsertTest(ConsistencyLevel.Three));
            Assert.AreEqual("Not enough replicas available for query at consistency Three (3 required but only 1 alive)", ex.Message);
        }

        [Test, Category("long")]
        public void Consistency_SimpleStatement_Three_SelectAndInsert()
        {
            int copies = 3;
            Dictionary<string, string> replication = new Dictionary<string, string> { { "class", ReplicationStrategies.SimpleStrategy }, { "replication_factor", copies.ToString() } };
            var testCluster = SetupSessionAndCluster(copies, replication);
            TestUtils.WaitForUp(testCluster.ClusterIpPrefix + "1", DefaultCassandraPort, 30);
            TestUtils.WaitForUp(testCluster.ClusterIpPrefix + "2", DefaultCassandraPort, 30);
            TestUtils.WaitForUp(testCluster.ClusterIpPrefix + "3", DefaultCassandraPort, 30);

            DoSimpleStatementSelectTest(ConsistencyLevel.Three);
            DoSimpleStatementInsertTest(ConsistencyLevel.Three);
        }

        [Test]
        public void Consistency_SimpleStatement_Three_Insert_NotEnoughReplicas()
        {
            SetupSessionAndCluster(_defaultNodeCountOne);
            var ex = Assert.Throws<UnavailableException>(() => DoSimpleStatementInsertTest(ConsistencyLevel.Three));
            Assert.AreEqual("Not enough replicas available for query at consistency Three (3 required but only 1 alive)", ex.Message);
        }

        [Test]
        public void Consistency_SimpleStatement_Two_Select_NotEnoughReplicas()
        {
            SetupSessionAndCluster(_defaultNodeCountOne);
            var ex = Assert.Throws<UnavailableException>(() => DoSimpleStatementInsertTest(ConsistencyLevel.Two));
            Assert.AreEqual("Not enough replicas available for query at consistency Two (2 required but only 1 alive)", ex.Message);
        }

        [Test, Category("long")]
        public void Consistency_SimpleStatement_Two_SelectAndInsert()
        {
            int copies = 2;
            Dictionary<string, string> replication = new Dictionary<string, string> { { "class", ReplicationStrategies.SimpleStrategy }, { "replication_factor", copies.ToString() } };
            var testCluster = SetupSessionAndCluster(copies, replication);
            TestUtils.WaitForUp(testCluster.ClusterIpPrefix + "1", DefaultCassandraPort, 30);
            TestUtils.WaitForUp(testCluster.ClusterIpPrefix + "2", DefaultCassandraPort, 30);

            DoSimpleStatementSelectTest(ConsistencyLevel.Two);
            DoSimpleStatementInsertTest(ConsistencyLevel.Two);
        }

        [Test]
        public void Consistency_SimpleStatement_Two_Insert_NotEnoughReplicas()
        {
            SetupSessionAndCluster(_defaultNodeCountOne);
            var ex = Assert.Throws<UnavailableException>(() => DoSimpleStatementInsertTest(ConsistencyLevel.Two));
            Assert.AreEqual("Not enough replicas available for query at consistency Two (2 required but only 1 alive)", ex.Message);
        }

        //////////////////////////////////////////////////
        /// Begin PreparedStatement Tests
        //////////////////////////////////////////////////

        [Test]
        public void Consistency_PreparedStatement_LocalSerial_Insert_Success()
        {
            ManyDataTypesEntity mdtp = ManyDataTypesEntity.GetRandomInstance();
            object[] vals = ConvertEntityToObjectArray(mdtp);
            SetupSessionAndCluster(_defaultNodeCountOne);

            PreparedStatement preparedInsertStatement = _session.Prepare(_preparedInsertStatementAsString);

            BoundStatement boundStatement = preparedInsertStatement.SetConsistencyLevel(ConsistencyLevel.Quorum).Bind(vals);
            boundStatement.SetSerialConsistencyLevel(ConsistencyLevel.LocalSerial);
            var result = _session.Execute(boundStatement);
            Assert.AreEqual(ConsistencyLevel.Quorum, result.Info.AchievedConsistency);

            var selectResult = _session.Execute(_defaultSelectStatement);
            Assert.AreEqual(_defaultPocoList.Count + 1, selectResult.GetRows().ToList().Count);
        }

        [Test]
        public void Consistency_PreparedStatement_LocalSerial_Insert_Fail()
        {
            SetupSessionAndCluster(_defaultNodeCountOne);
            //Serial not valid as normal consistency for writes
            Assert.Throws<InvalidQueryException>(() => DoPreparedInsertTest(ConsistencyLevel.LocalSerial));
        }

        /// <summary>
        /// Read and write consistency must be specified separately
        /// </summary>
        [Test]
        public void Consistency_PreparedStatement_LocalSerial_Select_Success()
        {
            SetupSessionAndCluster(_defaultNodeCountOne);

            PreparedStatement preparedSelectStatement = _session.Prepare(_defaultSelectStatement);
            BoundStatement statement = preparedSelectStatement.SetConsistencyLevel(ConsistencyLevel.Quorum).Bind();
            statement.SetSerialConsistencyLevel(ConsistencyLevel.LocalSerial);
            var result = _session.Execute(statement);
            Assert.AreEqual(ConsistencyLevel.Quorum, result.Info.AchievedConsistency);
            Assert.AreEqual(_defaultPocoList.Count, result.GetRows().ToList().Count);
        }

        [Test]
        public void Consistency_PreparedStatement_LocalSerial_Select_Does_Not_Throw()
        {
            SetupSessionAndCluster(_defaultNodeCountOne);
            Assert.DoesNotThrow(() => DoPreparedSelectTest(ConsistencyLevel.LocalSerial));
        }

        [Test]
        public void Consistency_PreparedStatement_Serial_Insert_Success()
        {
            ManyDataTypesEntity mdtp = ManyDataTypesEntity.GetRandomInstance();
            object[] vals = ConvertEntityToObjectArray(mdtp);
            SetupSessionAndCluster(_defaultNodeCountOne);

            PreparedStatement preparedInsertStatement = _session.Prepare(_preparedInsertStatementAsString);
            BoundStatement boundStatement = preparedInsertStatement.SetConsistencyLevel(ConsistencyLevel.Quorum).Bind(vals);
            boundStatement.SetSerialConsistencyLevel(ConsistencyLevel.Serial);
            var result = _session.Execute(boundStatement);
            Assert.AreEqual(ConsistencyLevel.Quorum, result.Info.AchievedConsistency);

            var selectResult = _session.Execute(_defaultSelectStatement);
            Assert.AreEqual(_defaultPocoList.Count + 1, selectResult.GetRows().ToList().Count);
        }

        [Test]
        public void Consistency_PreparedStatement_Serial_Insert_Fail()
        {
            SetupSessionAndCluster(_defaultNodeCountOne);
            Assert.Throws<InvalidQueryException>(() => DoPreparedInsertTest(ConsistencyLevel.Serial));
        }

        /// <summary>
        /// Read and write consistency must be specified separately
        /// </summary>
        [Test]
        public void Consistency_PreparedStatement_Serial_Select_Success()
        {
            SetupSessionAndCluster(_defaultNodeCountOne);

            PreparedStatement preparedSelectStatement = _session.Prepare(_defaultSelectStatement);
            BoundStatement statement = preparedSelectStatement.SetConsistencyLevel(ConsistencyLevel.Quorum).Bind();
            statement.SetSerialConsistencyLevel(ConsistencyLevel.Serial);
            var result = _session.Execute(statement);
            Assert.AreEqual(ConsistencyLevel.Quorum, result.Info.AchievedConsistency);
            Assert.AreEqual(_defaultPocoList.Count, result.GetRows().ToList().Count);

        }

        [Test]
        public void Consistency_PreparedStatement_Serial_Select_Fail()
        {
            SetupSessionAndCluster(_defaultNodeCountOne);
            Assert.DoesNotThrow(() => DoPreparedSelectTest(ConsistencyLevel.Serial));
        }

        [Test]
        public void Consistency_PreparedStatement_LocalOne_Select()
        {
            SetupSessionAndCluster(_defaultNodeCountOne);
            DoPreparedSelectTest(ConsistencyLevel.One);
        }

        [Test]
        public void Consistency_PreparedStatement_LocalOne_Insert()
        {
            SetupSessionAndCluster(_defaultNodeCountOne);
            DoPreparedInsertTest(ConsistencyLevel.One);
        }

        [Test]
        public void Consistency_PreparedStatement_Quorum_Select()
        {
            SetupSessionAndCluster(_defaultNodeCountOne);
            DoPreparedSelectTest(ConsistencyLevel.Quorum);
        }

        [Test]
        public void Consistency_PreparedStatement_Quorum_Insert()
        {
            SetupSessionAndCluster(_defaultNodeCountOne);
            DoPreparedInsertTest(ConsistencyLevel.Quorum);
        }

        [Test]
        public void Consistency_PreparedStatement_All_Select()
        {
            SetupSessionAndCluster(_defaultNodeCountOne);
            DoPreparedSelectTest(ConsistencyLevel.All);
        }

        [Test]
        public void Consistency_PreparedStatement_All_Insert()
        {
            SetupSessionAndCluster(_defaultNodeCountOne);
            DoPreparedInsertTest(ConsistencyLevel.All);
        }

        [Test]
        public void Consistency_PreparedStatement_Any_Select()
        {
            SetupSessionAndCluster(_defaultNodeCountOne);
            var ex = Assert.Throws<InvalidQueryException>(() => DoPreparedSelectTest(ConsistencyLevel.Any));
            Assert.AreEqual("ANY ConsistencyLevel is only supported for writes", ex.Message);
        }

        [Test]
        public void Consistency_PreparedStatement_Any_Insert()
        {
            SetupSessionAndCluster(_defaultNodeCountOne);
            DoPreparedInsertTest(ConsistencyLevel.Any);
        }

        [Test]
        public void Consistency_PreparedStatement_EachQuorum_SelectFails()
        {
            SetupSessionAndCluster(_defaultNodeCountOne);
            var ex = Assert.Throws<InvalidQueryException>(() => DoPreparedSelectTest(ConsistencyLevel.EachQuorum));
            Assert.AreEqual("EACH_QUORUM ConsistencyLevel is only supported for writes", ex.Message);
        }

        [Test]
        public void Consistency_PreparedStatement_LocalQuorum_Select()
        {
            SetupSessionAndCluster(_defaultNodeCountOne);
            DoPreparedSelectTest(ConsistencyLevel.LocalQuorum);
        }

        [Test]
        public void Consistency_PreparedStatement_EachQuorum_InsertSucceeds()
        {
            SetupSessionAndCluster(_defaultNodeCountOne);
            DoPreparedInsertTest(ConsistencyLevel.EachQuorum);
        }

        [Test]
        public void Consistency_PreparedStatement_LocalQuorum_Insert()
        {
            SetupSessionAndCluster(_defaultNodeCountOne);
            DoPreparedInsertTest(ConsistencyLevel.LocalQuorum);
        }

        [Test]
        public void Consistency_PreparedStatement_One_Select()
        {
            SetupSessionAndCluster(_defaultNodeCountOne);
            DoPreparedSelectTest(ConsistencyLevel.One);
        }

        [Test]
        public void Consistency_PreparedStatement_One_Insert()
        {
            SetupSessionAndCluster(_defaultNodeCountOne);
            DoPreparedInsertTest(ConsistencyLevel.One);
        }

        [Test]
        public void Consistency_PreparedStatement_Three_Select_NotEnoughReplicas()
        {
            SetupSessionAndCluster(_defaultNodeCountOne);
            var ex = Assert.Throws<UnavailableException>(() => DoPreparedInsertTest(ConsistencyLevel.Three));
            Assert.AreEqual("Not enough replicas available for query at consistency Three (3 required but only 1 alive)", ex.Message);
        }

        [Test, Category("long")]
        public void Consistency_PreparedStatement_Three_SelectAndInsert()
        {
            int copies = 3;
            Dictionary<string, string> replication = new Dictionary<string, string> { { "class", ReplicationStrategies.SimpleStrategy }, { "replication_factor", copies.ToString() } };
            var testCluster = SetupSessionAndCluster(copies, replication);
            TestUtils.WaitForUp(testCluster.ClusterIpPrefix + "1", DefaultCassandraPort, 30);
            TestUtils.WaitForUp(testCluster.ClusterIpPrefix + "2", DefaultCassandraPort, 30);
            TestUtils.WaitForUp(testCluster.ClusterIpPrefix + "3", DefaultCassandraPort, 30);

            DoPreparedSelectTest(ConsistencyLevel.Three);
            DoPreparedInsertTest(ConsistencyLevel.Three);
        }

        [Test]
        public void Consistency_PreparedStatement_Three_Insert_NotEnoughReplicas()
        {
            SetupSessionAndCluster(_defaultNodeCountOne);
            var ex = Assert.Throws<UnavailableException>(() => DoPreparedInsertTest(ConsistencyLevel.Three));
            Assert.AreEqual("Not enough replicas available for query at consistency Three (3 required but only 1 alive)", ex.Message);
        }

        [Test]
        public void Consistency_PreparedStatement_Two_Select_NotEnoughReplicas()
        {
            SetupSessionAndCluster(_defaultNodeCountOne);
            var ex = Assert.Throws<UnavailableException>(() => DoPreparedInsertTest(ConsistencyLevel.Two));
            Assert.AreEqual("Not enough replicas available for query at consistency Two (2 required but only 1 alive)", ex.Message);
        }

        [Test, Category("long")]
        public void Consistency_PreparedStatement_Two_SelectAndInsert()
        {
            int copies = 2;
            Dictionary<string, string> replication = new Dictionary<string, string> { { "class", ReplicationStrategies.SimpleStrategy }, { "replication_factor", copies.ToString() } };
            var testCluster = SetupSessionAndCluster(copies, replication);
            TestUtils.WaitForUp(testCluster.ClusterIpPrefix + "1", DefaultCassandraPort, 30);
            TestUtils.WaitForUp(testCluster.ClusterIpPrefix + "2", DefaultCassandraPort, 30);

            DoPreparedSelectTest(ConsistencyLevel.Two);
            DoPreparedInsertTest(ConsistencyLevel.Two);

        }

        [Test]
        public void Consistency_PreparedStatement_Two_Insert_NotEnoughReplicas()
        {
            SetupSessionAndCluster(_defaultNodeCountOne);
            var ex = Assert.Throws<UnavailableException>(() => DoPreparedInsertTest(ConsistencyLevel.Two));
            Assert.AreEqual("Not enough replicas available for query at consistency Two (2 required but only 1 alive)", ex.Message);
        }

        //////////////////////////////////////////////////
        /// Begin Batch Tests
        //////////////////////////////////////////////////

        [Test, TestCassandraVersion(2,0)]
        public void Consistency_Batch_All()
        {
            SetupSessionAndCluster(_defaultNodeCountOne);
            DoBatchInsertTest(ConsistencyLevel.All);
        }

        [Test, TestCassandraVersion(2, 0)]
        public void Consistency_Batch_Any()
        {
            SetupSessionAndCluster(_defaultNodeCountOne);
            DoBatchInsertTest(ConsistencyLevel.Any);
        }

        [Test, TestCassandraVersion(2, 0)]
        public void Consistency_Batch_EachQuorum()
        {
            SetupSessionAndCluster(_defaultNodeCountOne);
            DoBatchInsertTest(ConsistencyLevel.EachQuorum);
        }

        [Test, TestCassandraVersion(2, 0)]
        public void Consistency_Batch_LocalOne()
        {
            SetupSessionAndCluster(_defaultNodeCountOne);
            DoBatchInsertTest(ConsistencyLevel.LocalOne);
        }

        [Test, TestCassandraVersion(2, 0)]
        public void Consistency_Batch_LocalQuorum()
        {
            SetupSessionAndCluster(_defaultNodeCountOne);
            DoBatchInsertTest(ConsistencyLevel.LocalQuorum);
        }

        [Test, TestCassandraVersion(2, 0)]
        public void Consistency_Batch_LocalSerial()
        {
            SetupSessionAndCluster(_defaultNodeCountOne);
            DoBatchInsertTest(ConsistencyLevel.LocalSerial);
        }

        [Test, TestCassandraVersion(2, 0)]
        public void Consistency_Batch_One()
        {
            SetupSessionAndCluster(_defaultNodeCountOne);
            DoBatchInsertTest(ConsistencyLevel.One);
        }

        [Test, TestCassandraVersion(2, 0)]
        public void Consistency_Batch_Quorum()
        {
            SetupSessionAndCluster(_defaultNodeCountOne);
            DoBatchInsertTest(ConsistencyLevel.Quorum);
        }

        [Test, TestCassandraVersion(2, 0)]
        public void Consistency_Batch_Serial()
        {
            SetupSessionAndCluster(_defaultNodeCountOne);
            DoBatchInsertTest(ConsistencyLevel.Serial);
        }

        [Test, TestCassandraVersion(2, 0)]
        public void Consistency_Batch_Two()
        {
            int copies = 2;
            Dictionary<string, string> replication = new Dictionary<string, string> { { "class", ReplicationStrategies.SimpleStrategy }, { "replication_factor", copies.ToString() } };
            var testCluster = SetupSessionAndCluster(copies, replication);
            TestUtils.WaitForUp(testCluster.ClusterIpPrefix + "1", DefaultCassandraPort, 30);
            TestUtils.WaitForUp(testCluster.ClusterIpPrefix + "2", DefaultCassandraPort, 30);

            DoBatchInsertTest(ConsistencyLevel.Two);
        }

        [Test, TestCassandraVersion(2, 0)]
        public void Consistency_Batch_Three()
        {
            int copies = 3;
            Dictionary<string, string> replication = new Dictionary<string, string> { { "class", ReplicationStrategies.SimpleStrategy }, { "replication_factor", copies.ToString() } };
            var testCluster = SetupSessionAndCluster(copies, replication);
            TestUtils.WaitForUp(testCluster.ClusterIpPrefix + "1", DefaultCassandraPort, 30);
            TestUtils.WaitForUp(testCluster.ClusterIpPrefix + "2", DefaultCassandraPort, 30);
            TestUtils.WaitForUp(testCluster.ClusterIpPrefix + "3", DefaultCassandraPort, 30);

            DoBatchInsertTest(ConsistencyLevel.Three);
        }

        ///////////////////////////////////////
        ///  Test Helper Methods
        ///////////////////////////////////////

        private void DoBatchInsertTest(ConsistencyLevel expectedConsistencyLevel)
        {
            // Add a few more records via batch statement
            BatchStatement batch = new BatchStatement();
            batch.SetConsistencyLevel(expectedConsistencyLevel);
            var addlPocoList = ManyDataTypesEntity.GetDefaultAllDataTypesList();
            foreach (var manyDataTypesPoco in addlPocoList)
            {
                string simpleStatementStr = GetSimpleStatementInsertString(manyDataTypesPoco);
                SimpleStatement simpleStatement = new SimpleStatement(simpleStatementStr);
                batch.Add(simpleStatement);
            }

            // Validate Results
            var result = _session.Execute(batch);
            Assert.AreEqual(expectedConsistencyLevel, result.Info.AchievedConsistency);
            int totalExpectedRecords = _defaultPocoList.Count + addlPocoList.Count;
            result = _session.Execute(_defaultSelectStatement);
            Assert.AreEqual(totalExpectedRecords, result.GetRows().ToList().Count);
        }

        private void DoSimpleStatementSelectTest(ConsistencyLevel expectedConsistencyLevel)
        {
            SimpleStatement simpleStatement = (SimpleStatement)new SimpleStatement(_defaultSelectStatement).SetConsistencyLevel(expectedConsistencyLevel);
            var result = _session.Execute(simpleStatement);
            Assert.AreEqual(expectedConsistencyLevel, result.Info.AchievedConsistency);
            Assert.AreEqual(_defaultPocoList.Count, result.GetRows().ToList().Count);
        }

        private void DoSimpleStatementInsertTest(ConsistencyLevel expectedConsistencyLevel)
        {
            string simpleStatementStr = GetSimpleStatementInsertString(ManyDataTypesEntity.GetRandomInstance());
            SimpleStatement simpleStatement = new SimpleStatement(simpleStatementStr);
            simpleStatement = (SimpleStatement)simpleStatement.SetConsistencyLevel(expectedConsistencyLevel);
            var result = _session.Execute(simpleStatement);
            Assert.AreEqual(expectedConsistencyLevel, result.Info.AchievedConsistency);

            var selectResult = _session.Execute(_defaultSelectStatement);
            Assert.AreEqual(_defaultPocoList.Count + 1, selectResult.GetRows().ToList().Count);
        }

        private void DoPreparedSelectTest(ConsistencyLevel expectedConsistencyLevel)
        {
            // NOTE: We have to re-prepare every time since there is a unique Keyspace used for every test
            PreparedStatement preparedSelectStatement = _session.Prepare(_defaultSelectStatement);
            var result = _session.Execute(preparedSelectStatement.SetConsistencyLevel(expectedConsistencyLevel).Bind());
            Assert.AreEqual(expectedConsistencyLevel, result.Info.AchievedConsistency);
            Assert.AreEqual(_defaultPocoList.Count, result.GetRows().ToList().Count);
        }

        private void DoPreparedInsertTest(ConsistencyLevel expectedConsistencyLevel)
        {
            ManyDataTypesEntity mdtp = ManyDataTypesEntity.GetRandomInstance();
            object[] vals = ConvertEntityToObjectArray(mdtp);

            // NOTE: We have to re-prepare every time since there is a unique Keyspace used for every test
            PreparedStatement preparedInsertStatement = _session.Prepare(_preparedInsertStatementAsString).SetConsistencyLevel(expectedConsistencyLevel);
            BoundStatement boundStatement = preparedInsertStatement.Bind(vals);
            var result = _session.Execute(boundStatement);
            Assert.AreEqual(expectedConsistencyLevel, result.Info.AchievedConsistency);

            var selectResult = _session.Execute(_defaultSelectStatement);
            Assert.AreEqual(_defaultPocoList.Count + 1, selectResult.GetRows().ToList().Count);
        }

        private static object[] ConvertEntityToObjectArray(ManyDataTypesEntity mdtp)
        {
            // ToString() Example output: 
            // INSERT INTO "ManyDataTypesPoco" ("StringType", "GuidType", "DateTimeType", "DateTimeOffsetType", "BooleanType", 
            // "DecimalType", "DoubleType", "FloatType", "NullableIntType", "IntType", "Int64Type", 
            // "TimeUuidType", "NullableTimeUuidType", "DictionaryStringLongType", "DictionaryStringStringType", "ListOfGuidsType", "ListOfStringsType") VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)

            object[] vals =  
            {
                mdtp.StringType, mdtp.GuidType, mdtp.DateTimeType, mdtp.DateTimeOffsetType, mdtp.BooleanType, 
                mdtp.DecimalType, mdtp.DoubleType, mdtp.FloatType, mdtp.NullableIntType, mdtp.IntType, mdtp.Int64Type,
                // mdtp.TimeUuidType, 
                // mdtp.NullableTimeUuidType, 
                mdtp.DictionaryStringLongType, mdtp.DictionaryStringStringType, mdtp.ListOfGuidsType, mdtp.ListOfStringsType,
            };

            return vals;
        }
    }
}
