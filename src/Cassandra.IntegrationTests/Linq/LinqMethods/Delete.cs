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
using Renci.SshNet.Messages.Authentication;

namespace Cassandra.IntegrationTests.Linq.LinqMethods
{
    [Category("short")]
    public class Delete : TestGlobals
    {
        ISession _session = null;
        private List<AllDataTypesEntity> _entityList;
        string _uniqueKsName = TestUtils.GetUniqueKeyspaceName();
        private Table<AllDataTypesEntity> _table;

        [SetUp]
        public void SetupTest()
        {
            _session = TestClusterManager.GetTestCluster(1).Session;
            _session.CreateKeyspace(_uniqueKsName);
            _session.ChangeKeyspace(_uniqueKsName);

            _entityList = AllDataTypesEntity.SetupDefaultTable(_session);
            _table = new Table<AllDataTypesEntity>(_session, new MappingConfiguration());

        }

        [TearDown]
        public void TeardownTest()
        {
            _session.DeleteKeyspace(_uniqueKsName);
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
            AllDataTypesEntity entityToDelete = _entityList[0];

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

            var ex = Assert.Throws<InvalidQueryException>(() => deleteQuery.Execute());
            StringAssert.Contains("Non PRIMARY KEY boolean_type found in where clause", ex.Message);
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
            var table = new Table<AllDataTypesEntity>(_session, new MappingConfiguration());
            var ex = Assert.Throws<SyntaxError>(() => table.Delete().Execute());
            StringAssert.Contains("expecting K_WHERE", ex.Message);
        }

        /// <summary>
        /// Attempt to delete from a table without specifying a WHERE limiter.  Assert expected failure.
        /// NOTE: Not specifying a 'where' clause in C* is like Delete * in SQL, which is not allowed.
        /// </summary>
        [Test]
        public void Delete_MissingWhereClause_Sync()
        {
            var table = new Table<AllDataTypesEntity>(_session, new MappingConfiguration());
            var ex = Assert.Throws<Cassandra.SyntaxError>(() => table.Select(m => m).Delete().Execute());
            StringAssert.Contains("expecting K_WHERE", ex.Message);
        }

        /// <summary>
        /// 
        /// </summary>
        [Test]
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
        /// 
        /// </summary>
        [Test]
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

            var ex = Assert.Throws<InvalidQueryException>(() => deleteQuery.Execute());
            StringAssert.Contains(
                "DELETE statements must restrict all PRIMARY KEY columns with equality relations in order to use IF conditions, but column 'guid_type' is not restricted",
                ex.Message);

            // make sure record was not deleted
            count = table.Count().Execute();
            Assert.AreEqual(_entityList.Count, count);
        }


    }
}
