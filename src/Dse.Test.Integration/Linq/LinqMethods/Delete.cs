//
//  Copyright (C) 2017 DataStax, Inc.
//
//  Please see the license for details:
//  http://www.datastax.com/terms/datastax-dse-driver-license-terms
//

using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Cassandra.Data.Linq;
using Cassandra.IntegrationTests.Linq.Structures;
using Cassandra.IntegrationTests.TestBase;
using Cassandra.Mapping;
using NUnit.Framework;

namespace Cassandra.IntegrationTests.Linq.LinqMethods
{
    [Category("short")]
    public class Delete : SharedClusterTest
    {
        ISession _session;
        private List<AllDataTypesEntity> _entityList;
        private readonly string _uniqueKsName = TestUtils.GetUniqueKeyspaceName();
        private Table<AllDataTypesEntity> _table;

        [SetUp]
        public void SetupTest()
        {
            _session = Session;
            _session.CreateKeyspace(_uniqueKsName);
            _session.ChangeKeyspace(_uniqueKsName);

            _entityList = AllDataTypesEntity.SetupDefaultTable(_session);
            _table = new Table<AllDataTypesEntity>(_session, new MappingConfiguration());

        }

        [TearDown]
        public void TeardownTest()
        {
            TestUtils.TryToDeleteKeyspace(_session, _uniqueKsName);
        }

        [Test]
        public void Delete_DeleteOneEquals_Sync()
        {
            var table = new Table<AllDataTypesEntity>(_session, new MappingConfiguration());
            var count = table.Count().Execute();
            Assert.AreEqual(_entityList.Count, count);
            AllDataTypesEntity entityToDelete = _entityList[0];

            var selectQuery = table.Select(m => m).Where(m => m.StringType == entityToDelete.StringType);
            var deleteQuery = selectQuery.Delete();

            deleteQuery.Execute();
            count = table.Count().Execute();
            Assert.AreEqual(_entityList.Count - 1, count);
            Assert.AreEqual(0, selectQuery.Execute().ToList().Count);
        }

        [Test]
        public void Delete_DeleteOneViaEquals_Async()
        {
            var table = new Table<AllDataTypesEntity>(_session, new MappingConfiguration());
            var count = table.Count().Execute();
            Assert.AreEqual(_entityList.Count, count);
            AllDataTypesEntity entityToDelete = _entityList[0];

            var selectQuery = table.Select(m => m).Where(m => m.StringType == entityToDelete.StringType);
            var deleteQuery = selectQuery.Delete();

            deleteQuery.ExecuteAsync().Result.ToList();
            count = table.Count().Execute();
            Assert.AreEqual(_entityList.Count - 1, count);
            Assert.AreEqual(0, selectQuery.Execute().ToList().Count);
        }

        [Test]
        public void Delete_DeleteMultipleContains_Sync()
        {
            var table = new Table<AllDataTypesEntity>(_session, new MappingConfiguration());
            var count = table.Count().Execute();
            Assert.AreEqual(_entityList.Count, count);

            List<string> uniqueStringKeys = _entityList.Select(m => m.StringType).ToList();
            var deleteRequest = table.Where(m => uniqueStringKeys.Contains(m.StringType)).Delete();
            deleteRequest.Execute();
            count = table.Count().Execute();
            Assert.AreEqual(0, count);
        }

        [Test]
        public void Delete_DeleteMultipleContains_Async()
        {
            var table = new Table<AllDataTypesEntity>(_session, new MappingConfiguration());
            var count = table.Count().Execute();
            Assert.AreEqual(_entityList.Count, count);

            List<string> uniqueStringKeys = _entityList.Select(m => m.StringType).ToList();
            var deleteRequest = table.Where(m => uniqueStringKeys.Contains(m.StringType)).Delete();
            deleteRequest.ExecuteAsync().Result.ToList();
            count = table.Count().Execute();
            Assert.AreEqual(0, count);
        }

        [Test]
        public void Delete_MissingKey_Sync()
        {
            var table = new Table<AllDataTypesEntity>(_session, new MappingConfiguration());
            var count = table.Count().Execute();
            Assert.AreEqual(_entityList.Count, count);

            var selectQuery = table.Select(m => m).Where(m => m.BooleanType == true);
            var deleteQuery = selectQuery.Delete();

            Assert.Throws<InvalidQueryException>(() => deleteQuery.Execute());
        }

        [Test]
        public void Delete_NoSuchRecord()
        {
            var table = new Table<AllDataTypesEntity>(_session, new MappingConfiguration());
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
            var table = new Table<AllDataTypesEntity>(_session, new MappingConfiguration());
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
            var table = new Table<AllDataTypesEntity>(_session, new MappingConfiguration());
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
        [Test, TestCassandraVersion(2,1,2)]
        public void Delete_IfExists_ClusteringKeyOmitted()
        {
            var table = new Table<AllDataTypesEntity>(_session, new MappingConfiguration());
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


    }
}
