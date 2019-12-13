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
using System.Collections.Generic;
using System.Linq;

using Cassandra.Data.Linq;
using Cassandra.IntegrationTests.Linq.Structures;
using Cassandra.IntegrationTests.SimulacronAPI.PrimeBuilder.Then;
using Cassandra.IntegrationTests.TestBase;
using Cassandra.Mapping;

using NUnit.Framework;

namespace Cassandra.IntegrationTests.Linq.LinqMethods
{
    public class DeleteIf : SimulacronTest
    {
        private List<AllDataTypesEntity> _entityList = AllDataTypesEntity.GetDefaultAllDataTypesList();
        private readonly string _uniqueKsName = TestUtils.GetUniqueKeyspaceName();

        [Test, TestCassandraVersion(2, 0)]
        public void DeleteIf_ConditionSucceeds()
        {
            var table = new Table<Movie>(Session, new MappingConfiguration());
            Movie actualMovie = Movie.GetRandomMovie();

            TestCluster.PrimeFluent(
                b => b.WhenQuery(
                          $"DELETE FROM \"{Movie.TableName}\" WHERE \"unique_movie_title\" = ? AND \"movie_maker\" = ? AND \"director\" = ? IF \"mainGuy\" = ?",
                          when => when.WithParams(actualMovie.Title, actualMovie.MovieMaker, actualMovie.Director, actualMovie.MainActor))
                      .ThenRowsSuccess(new[] { "[applied]" }, rows => rows.WithRow(true)));

            var deleteIfStatement = table
                .Where(m => m.Title == actualMovie.Title && m.MovieMaker == actualMovie.MovieMaker && m.Director == actualMovie.Director)
                .DeleteIf(m => m.MainActor == actualMovie.MainActor);

            var appliedInfo = deleteIfStatement.Execute();
            Assert.True(appliedInfo.Applied);
        }

        [Test, TestCassandraVersion(2, 0)]
        public void DeleteIf_ConditionFails()
        {
            var random = ConstantReturningHelper.FromObj(Randomm.RandomAlphaNum(16));
            var table = new Table<Movie>(Session, new MappingConfiguration());
            Movie actualMovie = Movie.GetRandomMovie();

            TestCluster.PrimeFluent(
                b => b.WhenQuery(
                          $"DELETE FROM \"{Movie.TableName}\" WHERE \"unique_movie_title\" = ? AND \"movie_maker\" = ? AND \"director\" = ? IF \"mainGuy\" = ?",
                          when => when.WithParams(actualMovie.Title, actualMovie.MovieMaker, actualMovie.Director, random.Get()))
                      .ThenRowsSuccess(actualMovie.CreateAppliedInfoRowsResult()));

            var deleteIfStatement = table
                                    .Where(m => m.Title == actualMovie.Title && m.MovieMaker == actualMovie.MovieMaker && m.Director == actualMovie.Director)
                                    .DeleteIf(m => m.MainActor == random.Get());

            var appliedInfo = deleteIfStatement.Execute();
            Assert.False(appliedInfo.Applied);
            Assert.NotNull(appliedInfo.Existing);
            Assert.AreEqual(actualMovie.MainActor, appliedInfo.Existing.MainActor);
        }

        [Test, TestCassandraVersion(2, 1, 2)]
        public void DeleteIf_ConditionBasedOnKey()
        {
            var table = new Table<AllDataTypesEntity>(Session, new MappingConfiguration());
            var entityToDelete = _entityList[0];
            var expectedErrMsg = "PRIMARY KEY column 'string_type' cannot have IF conditions";
            
            TestCluster.PrimeFluent(
                b => b.WhenQuery($"DELETE FROM \"{AllDataTypesEntity.TableName}\" WHERE \"string_type\" = ? IF \"string_type\" = ?",
                          when => when.WithParams(entityToDelete.StringType, entityToDelete.StringType))
                      .ThenServerError(ServerError.Invalid, expectedErrMsg));

            var selectQuery = table.Select(m => m).Where(m => m.StringType == entityToDelete.StringType);
            var deleteIfQuery = selectQuery.DeleteIf(m => m.StringType == entityToDelete.StringType);
            try
            {
                deleteIfQuery.Execute();
                Assert.Fail("Expected exception was not thrown!");
            }
            catch (InvalidQueryException e)
            {
                Assert.AreEqual(expectedErrMsg, e.Message);
            }
        }

        [Test, TestCassandraVersion(2, 1, 2)]
        public void DeleteIf_NotAllKeysRestricted_ClusteringKeyOmitted()
        {
            var random = ConstantReturningHelper.FromObj(Randomm.RandomAlphaNum(10));
            var table = new Table<AllDataTypesEntity>(Session, new MappingConfiguration());
            var entityToDelete = _entityList[0];
            
            TestCluster.PrimeFluent(
                b => b.WhenQuery($"DELETE FROM \"{AllDataTypesEntity.TableName}\" WHERE \"string_type\" = ? IF \"int_type\" = ?",
                          when => when.WithParams(entityToDelete.StringType + random.Get(), entityToDelete.IntType))
                      .ThenServerError(ServerError.Invalid, "msg"));

            // Test
            var selectQuery = table.Select(m => m).Where(m => m.StringType == entityToDelete.StringType + random.Get());
            var deleteIfQuery = selectQuery.DeleteIf(m => m.IntType == entityToDelete.IntType);
            Assert.Throws<InvalidQueryException>(() => deleteIfQuery.Execute());
        }

        [Test, TestCassandraVersion(2, 1, 2)]
        public void DeleteIf_NotAllKeysRestricted_PartitionKeyOmitted()
        {
            var random = ConstantReturningHelper.FromObj(Guid.NewGuid());
            var table = new Table<AllDataTypesEntity>(Session, new MappingConfiguration());
            AllDataTypesEntity entityToDelete = _entityList[0];

            TestCluster.PrimeFluent(
                b => b.WhenQuery($"DELETE FROM \"{AllDataTypesEntity.TableName}\" WHERE \"guid_type\" = ? IF \"int_type\" = ?",
                          when => when.WithParams(random.Get(), entityToDelete.IntType))
                      .ThenServerError(ServerError.Invalid, "msg"));

            // Test
            var selectQuery = table.Select(m => m).Where(m => m.GuidType == random.Get());
            var deleteIfQuery = selectQuery.DeleteIf(m => m.IntType == entityToDelete.IntType);

            Assert.Throws<InvalidQueryException>(() => deleteIfQuery.Execute());
        }

        [Test, TestCassandraVersion(2, 0)]
        public void DeleteIf_NoMatchingRecord()
        {
            var random = ConstantReturningHelper.FromObj(Randomm.RandomAlphaNum(10));
            var randomGuid = ConstantReturningHelper.FromObj(Guid.NewGuid());
            var table = new Table<AllDataTypesEntity>(Session, new MappingConfiguration());
            AllDataTypesEntity entityToDelete = _entityList[0];

            TestCluster.PrimeFluent(
                b => b.WhenQuery($"DELETE FROM \"{AllDataTypesEntity.TableName}\" WHERE \"string_type\" = ? AND \"guid_type\" = ? IF \"int_type\" = ?",
                          when => when.WithParams(entityToDelete.StringType + random.Get(), randomGuid.Get(), entityToDelete.IntType))
                      .ThenRowsSuccess(new [] { "[applied]" }, rows => rows.WithRow(false)));

            // Test
            var selectQuery = table.Select(m => m)
                                   .Where(m => m.StringType == entityToDelete.StringType + random.Get() 
                                               && m.GuidType == randomGuid.Get());
            var deleteIfQuery = selectQuery.DeleteIf(m => m.IntType == entityToDelete.IntType);

            var appliedInfo = deleteIfQuery.Execute();
            Assert.False(appliedInfo.Applied);
            Assert.IsNull(appliedInfo.Existing);
        }
    }
}