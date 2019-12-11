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

using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Cassandra.Data.Linq;
using Cassandra.IntegrationTests.Linq.Structures;
using Cassandra.IntegrationTests.TestBase;
using Cassandra.Mapping;

using NUnit.Framework;

namespace Cassandra.IntegrationTests.Linq.LinqMethods
{
    [Category("short")]
    public class Delete : SimulacronTest
    {
        private List<AllDataTypesEntity> _entityList;
        private readonly string _uniqueKsName = TestUtils.GetUniqueKeyspaceName();
        private Table<AllDataTypesEntity> _table;

        [SetUp]
        public override void SetupTest()
        {
            base.SetupTest();
            _entityList = AllDataTypesEntity.GetDefaultAllDataTypesList();
            _table = new Table<AllDataTypesEntity>(Session, new MappingConfiguration());
        }
        
        [TestCase(true)]
        [TestCase(false)]
        [Test]
        public void Delete_DeleteOneEquals(bool async)
        {
            var table = new Table<AllDataTypesEntity>(Session, new MappingConfiguration());

            AllDataTypesEntity.PrimeCountQuery(TestCluster, _entityList.Count);
            var count = table.Count().Execute();
            Assert.AreEqual(_entityList.Count, count);

            AllDataTypesEntity entityToDelete = _entityList[0];

            var selectQuery = table.Select(m => m).Where(m => m.StringType == entityToDelete.StringType);
            var deleteQuery = selectQuery.Delete();

            if (async)
            {
                deleteQuery.ExecuteAsync().GetAwaiter().GetResult();
            }
            else
            {
                deleteQuery.Execute();
            }

            VerifyBoundStatement(
                $"DELETE FROM \"{AllDataTypesEntity.TableName}\" WHERE \"string_type\" = ?", 1, entityToDelete.StringType);
            
            TestCluster.PrimeDelete();
            AllDataTypesEntity.PrimeCountQuery(TestCluster, _entityList.Count - 1);

            count = table.Count().Execute();
            Assert.AreEqual(_entityList.Count - 1, count);
            TestCluster.PrimeFluent(b => entityToDelete.When(TestCluster, b).ThenVoidSuccess());
            Assert.AreEqual(0, selectQuery.Execute().ToList().Count);
        }
        
        [TestCase(true)]
        [TestCase(false)]
        [Test]
        public void Delete_DeleteMultipleContains(bool async)
        {
            var table = new Table<AllDataTypesEntity>(Session, new MappingConfiguration());

            var uniqueStringKeys = _entityList.Select(m => m.StringType).ToList();
            var deleteRequest = table.Where(m => uniqueStringKeys.Contains(m.StringType)).Delete();
            if (async)
            {
                deleteRequest.ExecuteAsync().GetAwaiter().GetResult();
            }
            else
            {
                deleteRequest.Execute();
            }

            VerifyBoundStatement(
                $"DELETE FROM \"{AllDataTypesEntity.TableName}\" WHERE \"string_type\" IN ?", 1, uniqueStringKeys);
        }

        [Test]
        public void Delete_MissingKey_Sync()
        {
            var table = new Table<AllDataTypesEntity>(Session, new MappingConfiguration());
            var count = table.Count().Execute();
            Assert.AreEqual(_entityList.Count, count);

            var selectQuery = table.Select(m => m).Where(m => m.BooleanType == true);
            var deleteQuery = selectQuery.Delete();

            Assert.Throws<InvalidQueryException>(() => deleteQuery.Execute());
        }

        [Test]
        public void Delete_NoSuchRecord()
        {
            var table = new Table<AllDataTypesEntity>(Session, new MappingConfiguration());
            var count = table.Count().Execute();
            Assert.AreEqual(_entityList.Count, count);
            AllDataTypesEntity entityToDelete = _entityList[0];
            var selectQuery = table.Select(m => m).Where(m => m.StringType == entityToDelete.StringType + Randomm.RandomAlphaNum(16));
            var deleteQuery = selectQuery.Delete();
            deleteQuery.Execute();

            // make sure record was not deleted
            count = table.Count().Execute();
            Assert.AreEqual(_entityList.Count, count);
            List<AllDataTypesEntity> rows = table.Select(m => m).Where(m => m.StringType == entityToDelete.StringType).Execute().ToList();
            Assert.AreEqual(1, rows.Count);
        }

        /// <summary>
        /// Attempt to delete from a table without specifying a WHERE limiter.  Assert expected failure.
        /// NOTE: Not specifying a 'where' clause in C* is like Delete * in SQL, which is not allowed.
        /// </summary>
        [Test]
        public void Delete_MissingWhereAndSelectClause_Sync()
        {
            Assert.Throws<SyntaxError>(() => _table.Delete().Execute());
        }

        /// <summary>
        /// Attempt to delete from a table without specifying a WHERE limiter.  Assert expected failure.
        /// NOTE: Not specifying a 'where' clause in C* is like Delete * in SQL, which is not allowed.
        /// </summary>
        [Test]
        public void Delete_MissingWhereClause_Sync()
        {
            Assert.Throws<SyntaxError>(() => _table.Select(m => m).Delete().Execute());
        }

        /// <summary>
        /// Successfully delete a record using the IfExists condition
        /// </summary>
        [Test, TestCassandraVersion(2, 0)]
        public void Delete_IfExists()
        {
            var table = new Table<AllDataTypesEntity>(Session, new MappingConfiguration());
            var count = table.Count().Execute();
            Assert.AreEqual(_entityList.Count, count);
            AllDataTypesEntity entityToDelete = _entityList[0];

            var selectQuery = table.Select(m => m).Where(m => m.StringType == entityToDelete.StringType && m.GuidType == entityToDelete.GuidType);
            var deleteQuery = selectQuery.Delete().IfExists();

            deleteQuery.Execute();
            count = table.Count().Execute();
            Assert.AreEqual(_entityList.Count - 1, count);
            Assert.AreEqual(0, selectQuery.Execute().ToList().Count);
        }

        /// <summary>
        /// Successfully delete a record using the IfExists condition, when the row doesn't exist.
        /// </summary>
        [Test, TestCassandraVersion(2, 0)]
        public void Delete_IfExists_RowDoesntExist()
        {
            var table = new Table<AllDataTypesEntity>(Session, new MappingConfiguration());
            var count = table.Count().Execute();
            Assert.AreEqual(_entityList.Count, count);
            AllDataTypesEntity entityToDelete = _entityList[0];

            var selectQuery = table.Select(m => m).Where(m => m.StringType == entityToDelete.StringType && m.GuidType == entityToDelete.GuidType);
            var deleteQuery = selectQuery.Delete().IfExists();

            deleteQuery.Execute();
            count = table.Count().Execute();
            Assert.AreEqual(_entityList.Count - 1, count);
            Assert.AreEqual(0, selectQuery.Execute().ToList().Count);

            // Executing again should not fail, should just be a no-op
            deleteQuery.Execute();
            count = table.Count().Execute();
            Assert.AreEqual(_entityList.Count - 1, count);
            Assert.AreEqual(0, selectQuery.Execute().ToList().Count);
        }

        /// <summary>
        ///
        /// </summary>
        [Test, TestCassandraVersion(2, 1, 2)]
        public void Delete_IfExists_ClusteringKeyOmitted()
        {
            var table = new Table<AllDataTypesEntity>(Session, new MappingConfiguration());
            var count = table.Count().Execute();
            Assert.AreEqual(_entityList.Count, count);
            AllDataTypesEntity entityToDelete = _entityList[0];

            var selectQuery = table.Select(m => m).Where(m => m.StringType == entityToDelete.StringType);
            var deleteQuery = selectQuery.Delete().IfExists();

            Assert.Throws<InvalidQueryException>(() => deleteQuery.Execute());

            // make sure record was not deleted
            count = table.Count().Execute();
            Assert.AreEqual(_entityList.Count, count);
        }

        [TestCase(BatchType.Unlogged)]
        [TestCase(BatchType.Logged)]
        [TestCase(null)]
        [TestCassandraVersion(2, 0)]
        public void Delete_BatchType(BatchType batchType)
        {
            var table = new Table<AllDataTypesEntity>(Session, new MappingConfiguration());
            var count = table.Count().Execute();
            Assert.AreEqual(_entityList.Count, count);
            AllDataTypesEntity entityToDelete = _entityList[0];
            AllDataTypesEntity entityToDelete2 = _entityList[1];

            var selectQuery = table.Select(m => m).Where(m => m.StringType == entityToDelete.StringType);
            var deleteQuery = selectQuery.Delete();
            var selectQuery2 = table.Select(m => m).Where(m => m.StringType == entityToDelete2.StringType);
            var deleteQuery2 = selectQuery2.Delete();

            var batch = table.GetSession().CreateBatch(batchType);
            batch.Append(deleteQuery);
            batch.Append(deleteQuery2);
            batch.Execute();

            count = table.Count().Execute();
            Assert.AreEqual(_entityList.Count - 2, count);
            Assert.AreEqual(0, selectQuery.Execute().ToList().Count);
            Assert.AreEqual(0, selectQuery2.Execute().ToList().Count);
        }
    }
}