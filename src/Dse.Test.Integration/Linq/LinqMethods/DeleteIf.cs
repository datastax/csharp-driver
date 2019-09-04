//
//  Copyright (C) DataStax, Inc.
//
//  Please see the license for details:
//  http://www.datastax.com/terms/datastax-dse-driver-license-terms
//

using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Dse.Data.Linq;
using Dse.Test.Integration.Linq.Structures;
using Dse.Test.Integration.TestClusterManagement;
using NUnit.Framework;
using Dse.Mapping;

namespace Dse.Test.Integration.Linq.LinqMethods
{
    [Category("short"), Category("realcluster")]
    public class DeleteIf : SharedClusterTest
    {
        ISession _session = null;
        private List<AllDataTypesEntity> _entityList;
        private readonly string _uniqueKsName = TestUtils.GetUniqueKeyspaceName();

        [SetUp]
        public void SetupTest()
        {
            _session = Session;
            _session.CreateKeyspace(_uniqueKsName);
            _session.ChangeKeyspace(_uniqueKsName);

            _entityList = AllDataTypesEntity.SetupDefaultTable(_session);

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

            var appliedInfo = deleteIfStatement.Execute();
            Assert.True(appliedInfo.Applied);
            Assert.Null(appliedInfo.Existing);
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

            var appliedInfo = deleteIfStatement.Execute();
            Assert.False(appliedInfo.Applied);
            Assert.NotNull(appliedInfo.Existing);
            Assert.AreEqual(actualMovie.MainActor, appliedInfo.Existing.MainActor);
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
            Assert.Throws<InvalidQueryException>(() => deleteIfQuery.Execute());
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

            Assert.Throws<InvalidQueryException>(() => deleteIfQuery.Execute());
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

            var appliedInfo = deleteIfQuery.Execute();
            Assert.False(appliedInfo.Applied);
        }


    }
}
