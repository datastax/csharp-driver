using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Cassandra.Data.Linq;
using Cassandra.IntegrationTests.Linq.Structures;
using Cassandra.IntegrationTests.TestBase;
using NUnit.Framework;
using Renci.SshNet.Messages.Authentication;
using Cassandra.Mapping;

namespace Cassandra.IntegrationTests.Linq.LinqMethods
{
    [Category("short")]
    public class DeleteIf : TestGlobals
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
            TestUtils.TryToDeleteKeyspace(_session, _uniqueKsName);
        }

        [Test, TestCassandraVersion(2, 0)]
        public void DeleteIf_ConditionSucceeds()
        {
            var table = new Table<Movie>(_session, new MappingConfiguration());
            table.Create();
            Movie actualMovie = Movie.GetRandomMovie();
            table.Insert(actualMovie).Execute();
            long count = table.Count().Execute();
            Assert.AreEqual(1, count);

            var deleteIfStatement = table
                .Where(m => m.Title == actualMovie.Title && m.MovieMaker == actualMovie.MovieMaker && m.Director == actualMovie.Director)
                .DeleteIf(m => m.MainActor == actualMovie.MainActor);

            deleteIfStatement.Execute();
            count = -1;
            count = table.Count().Execute();
            Assert.AreEqual(0, count);
        }

        [Test, TestCassandraVersion(2, 0)]
        public void DeleteIf_ConditionFails()
        {
            var table = new Table<Movie>(_session, new MappingConfiguration());
            table.Create();
            Movie actualMovie = Movie.GetRandomMovie();
            table.Insert(actualMovie).Execute();
            long count = table.Count().Execute();
            Assert.AreEqual(1, count);

            var deleteIfStatement = table
                .Where(m => m.Title == actualMovie.Title && m.MovieMaker == actualMovie.MovieMaker && m.Director == actualMovie.Director)
                .DeleteIf(m => m.MainActor == Randomm.RandomAlphaNum(16));

            deleteIfStatement.Execute();
            count = -1;
            count = table.Count().Execute();
            Assert.AreEqual(1, count);
        }

        [Test, TestCassandraVersion(2, 1, 2)]
        public void DeleteIf_ConditionBasedOnKey()
        {
            var table = new Table<AllDataTypesEntity>(_session, new MappingConfiguration());
            var count = table.Count().Execute();
            Assert.AreEqual(_entityList.Count, count);
            AllDataTypesEntity entityToDelete = _entityList[0];
            var selectQuery = table.Select(m => m).Where(m => m.StringType == entityToDelete.StringType);
            var deleteIfQuery = selectQuery.DeleteIf(m => m.StringType == entityToDelete.StringType);
            try
            {
                deleteIfQuery.Execute();
                Assert.Fail("Expected exception was not thrown!");
            }
            catch (InvalidQueryException e)
            {
                string expectedErrMsg = "PRIMARY KEY column 'string_type' cannot have IF conditions";
                Assert.AreEqual(expectedErrMsg, e.Message);
            }
            // make sure record was not deleted
            count = table.Count().Execute();
            Assert.AreEqual(_entityList.Count, count);
            List<AllDataTypesEntity> rows = selectQuery.Execute().ToList();
            Assert.AreEqual(1, rows.Count);
        }

        [Test, TestCassandraVersion(2, 1, 2)]
        public void DeleteIf_NotAllKeysRestricted_ClusteringKeyOmitted()
        {
            // Validate pre-test state
            var table = new Table<AllDataTypesEntity>(_session, new MappingConfiguration());
            var count = table.Count().Execute();
            Assert.AreEqual(_entityList.Count, count);
            AllDataTypesEntity entityToDelete = _entityList[0];

            // Test
            var selectQuery = table.Select(m => m).Where(m => m.StringType == entityToDelete.StringType + Randomm.RandomAlphaNum(10));
            var deleteIfQuery = selectQuery.DeleteIf(m => m.IntType == entityToDelete.IntType);
            var ex = Assert.Throws<InvalidQueryException>(() => deleteIfQuery.Execute());
            StringAssert.Contains(
                "DELETE statements must restrict all PRIMARY KEY columns with equality relations in order to use IF conditions, but column 'guid_type' is not restricted",
                ex.Message);
        }

        [Test, TestCassandraVersion(2, 1, 2)]
        public void DeleteIf_NotAllKeysRestricted_PartitionKeyOmitted()
        {
            // Validate pre-test state
            var table = new Table<AllDataTypesEntity>(_session, new MappingConfiguration());
            var count = table.Count().Execute();
            Assert.AreEqual(_entityList.Count, count);
            AllDataTypesEntity entityToDelete = _entityList[0];

            // Test
            var selectQuery = table.Select(m => m).Where(m => m.GuidType == Guid.NewGuid());
            var deleteIfQuery = selectQuery.DeleteIf(m => m.IntType == entityToDelete.IntType);

            var ex = Assert.Throws<InvalidQueryException>(() => deleteIfQuery.Execute());
            StringAssert.Contains(
                "DELETE statements must restrict all PRIMARY KEY columns with equality relations in order to use IF conditions, but column 'string_type' is not restricted",
                ex.Message);
        }

        [Test, TestCassandraVersion(2, 0)]
        public void DeleteIf_NoMatchingRecord()
        {
            // Validate pre-test state
            var table = new Table<AllDataTypesEntity>(_session, new MappingConfiguration());
            var count = table.Count().Execute();
            Assert.AreEqual(_entityList.Count, count);
            AllDataTypesEntity entityToDelete = _entityList[0];

            // Test
            var selectQuery = table.Select(m => m).Where(m => m.StringType == entityToDelete.StringType + Randomm.RandomAlphaNum(10) && m.GuidType == Guid.NewGuid());
            var deleteIfQuery = selectQuery.DeleteIf(m => m.IntType == entityToDelete.IntType);

            string deleteIfQueryToString = deleteIfQuery.ToString();
            Console.WriteLine(deleteIfQueryToString);

            Assert.DoesNotThrow(() => deleteIfQuery.Execute());
        }


    }
}
