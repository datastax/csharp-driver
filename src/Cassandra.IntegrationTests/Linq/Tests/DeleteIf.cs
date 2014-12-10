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

namespace Cassandra.IntegrationTests.Linq.Tests
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
            _table = _session.GetTable<AllDataTypesEntity>();

        }

        [TearDown]
        public void TeardownTest()
        {
            _session.DeleteKeyspace(_uniqueKsName);
        }

        [Test]
        public void DeleteIf_ConditionSucceeds()
        {
            var table = _session.GetTable<Movie>();
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

        [Test]
        public void DeleteIf_ConditionFails()
        {
            var table = _session.GetTable<Movie>();
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

        [Test]
        public void DeleteIf_ConditionBasedOnKey()
        {
            var table = _session.GetTable<AllDataTypesEntity>();
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
                string expectedErrMsg = "PRIMARY KEY part string_type found in SET part";
                Assert.AreEqual(expectedErrMsg, e.Message);
            }
            // make sure record was not deleted
            count = table.Count().Execute();
            Assert.AreEqual(_entityList.Count, count);
            List<AllDataTypesEntity> rows = selectQuery.Execute().ToList();
            Assert.AreEqual(1, rows.Count);
        }

        [Test]
        public void DeleteIf_NoSuchKey()
        {
            // Validate pre-test state
            var table = _session.GetTable<AllDataTypesEntity>();
            var count = table.Count().Execute();
            Assert.AreEqual(_entityList.Count, count);
            AllDataTypesEntity entityToDelete = _entityList[0];

            // Test
            var selectQuery = table.Select(m => m).Where(m => m.StringType == entityToDelete.StringType + Randomm.RandomAlphaNum(10));
            var deleteIfQuery = selectQuery.DeleteIf(m => m.IntType == entityToDelete.IntType);
            deleteIfQuery.Execute();

            // Validate post-test state
            count = table.Count().Execute();
            Assert.AreEqual(_entityList.Count, count);
            List<AllDataTypesEntity> rows = table.Select(m => m).Where(m => m.StringType == entityToDelete.StringType).Execute().ToList();
            Assert.AreEqual(1, rows.Count);
        }


    }
}
