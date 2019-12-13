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
using System.Threading.Tasks;

using Cassandra.Data.Linq;
using Cassandra.IntegrationTests.Linq.Structures;
using Cassandra.IntegrationTests.SimulacronAPI;
using Cassandra.IntegrationTests.SimulacronAPI.PrimeBuilder.Then;
using Cassandra.IntegrationTests.TestBase;
using Cassandra.Mapping;
using Cassandra.Tests.Mapping.Pocos;

using NUnit.Framework;

#pragma warning disable 612

namespace Cassandra.IntegrationTests.Linq.LinqMethods
{
    public class InsertTests : SimulacronTest
    {
        private static readonly string UniqueKsName = TestUtils.GetUniqueKeyspaceName();

        public InsertTests() : base(keyspace: InsertTests.UniqueKsName)
        {
        }

        [Test, TestCassandraVersion(2, 0)]
        public void LinqInsert_Batch_Test()
        {
            Table<Movie> nerdMoviesTable = new Table<Movie>(Session, new MappingConfiguration(), Movie.TableName, InsertTests.UniqueKsName);
            Batch batch = Session.CreateBatch();

            Movie movie1 = Movie.GetRandomMovie();
            Movie movie2 = Movie.GetRandomMovie();
            movie1.Director = "Joss Whedon";
            var movies = new List<Movie>
            {
                movie1,
                movie2,
            };

            batch.Append(from m in movies select nerdMoviesTable.Insert(m));
            Task taskSaveMovies = Task.Factory.FromAsync(batch.BeginExecute, batch.EndExecute, null);
            taskSaveMovies.Wait();

            VerifyBatchStatement(
                1,
                new[]
                {
                    $"INSERT INTO \"{InsertTests.UniqueKsName}\".\"{Movie.TableName}\" (\"director\", \"list\", \"mainGuy\", \"movie_maker\", \"unique_movie_title\", \"yearMade\") VALUES (?, ?, ?, ?, ?, ?)",
                    $"INSERT INTO \"{InsertTests.UniqueKsName}\".\"{Movie.TableName}\" (\"director\", \"list\", \"mainGuy\", \"movie_maker\", \"unique_movie_title\", \"yearMade\") VALUES (?, ?, ?, ?, ?, ?)",
                },
                new[]
                {
                    movie1.GetParameters(),
                    movie2.GetParameters()
                });
        }

        [Test, TestCassandraVersion(2, 0)]
        public void LinqInsert_WithSetTimestamp_Test()
        {
            var nerdMoviesTable = new Table<Movie>(Session, new MappingConfiguration());
            var movie1 = Movie.GetRandomMovie();
            nerdMoviesTable.Insert(movie1).Execute();

            var mainActor = "Samuel L. Jackson";
            movie1.MainActor = mainActor;
            var dt = DateTime.Now.AddDays(1);

            nerdMoviesTable
                .Insert(movie1)
                .SetTimestamp(dt)
                .Execute();

            VerifyBoundStatement(
                $"INSERT INTO \"{Movie.TableName}\" " +
                    "(\"director\", \"list\", \"mainGuy\", \"movie_maker\", \"unique_movie_title\", \"yearMade\") " +
                "VALUES " +
                    "(?, ?, ?, ?, ?, ?) USING TIMESTAMP ?",
                1,
                movie1.GetParameters().Concat(new object[] { DataType.GetMicroSecondsTimestamp(dt) }).ToArray());
        }

        [Test]
        public void Insert_Mapping_Attributes_Test()
        {
            var table = new Table<AttributeMappingClass>(Session, new MappingConfiguration());
            var obj = new AttributeMappingClass
            {
                PartitionKey = 1,
                ClusteringKey0 = 2L,
                ClusteringKey1 = "3",
                ClusteringKey2 = Guid.NewGuid()
            };
            table.Insert(obj).Execute();

            VerifyBoundStatement(
                "INSERT INTO attr_mapping_class_table " +
                    "(bool_value_col, clustering_key_0, clustering_key_1, clustering_key_2, decimal_value_col, float_value_col, partition_key) " +
                "VALUES " +
                "(?, ?, ?, ?, ?, ?, ?)",
                1,
                obj.GetParameters());
        }

        /// <summary>
        /// Testing the CQLGenerator insert query without null values.
        /// Testing if the query has a correct number of placeholders in case of the first column value is null.
        ///
        /// @jira CSHARP-451 https://datastax-oss.atlassian.net/browse/CSHARP-451
        ///
        /// </summary>
        [Test, TestCassandraVersion(2, 0)]
        public void LinqInsert_WithNullInFirstColumnValue_Test()
        {
            Table<Movie> nerdMoviesTable = new Table<Movie>(Session, new MappingConfiguration());
            Movie movie1 = Movie.GetRandomMovie();
            movie1.MainActor = null; //Setting first column
            nerdMoviesTable.Insert(movie1, false).Execute();

            VerifyBoundStatement(
                $"INSERT INTO \"{Movie.TableName}\" " +
                "(\"director\", \"list\", \"movie_maker\", \"unique_movie_title\", \"yearMade\") " +
                "VALUES " +
                "(?, ?, ?, ?, ?)",
                1,
                movie1.GetParameters(false).ToArray());
        }

        [Test, TestCassandraVersion(2, 0)]
        public void LinqInsert_Batch_MissingPartitionKeyPart_Test()
        {
            Table<Movie> nerdMoviesTable = new Table<Movie>(Session, new MappingConfiguration());
            Batch batch = Session.CreateBatch();
            Movie movie1 = Movie.GetRandomMovie();
            Movie movie2 = Movie.GetRandomMovie();
            movie1.MovieMaker = null; // missing partition key
            var movies = new List<Movie>
            {
                movie1,
                movie2,
            };

            batch.Append(from m in movies select nerdMoviesTable.Insert(m));
            Task taskSaveMovies = Task.Factory.FromAsync(batch.BeginExecute, batch.EndExecute, null);

            taskSaveMovies.Wait();

            VerifyBatchStatement(
                1,
                new[]
                {
                    $"INSERT INTO \"{Movie.TableName}\" (\"director\", \"list\", \"mainGuy\", \"movie_maker\", \"unique_movie_title\", \"yearMade\") VALUES (?, ?, ?, ?, ?, ?)",
                    $"INSERT INTO \"{Movie.TableName}\" (\"director\", \"list\", \"mainGuy\", \"movie_maker\", \"unique_movie_title\", \"yearMade\") VALUES (?, ?, ?, ?, ?, ?)",
                },
                new[]
                {
                    movie1.GetParameters(),
                    movie2.GetParameters()
                });
        }

        [Test]
        public void LinqInsert_MissingPartitionKey_Sync_Test()
        {
            var table = new Table<Movie>(Session, new MappingConfiguration());
            Movie objectMissingPartitionKey = new Movie() { MainActor = "doesntmatter" };

            TestCluster.PrimeFluent(
                b => b.WhenQuery(
                          $"INSERT INTO \"{Movie.TableName}\" (\"director\", \"list\", \"mainGuy\", \"movie_maker\", \"unique_movie_title\", \"yearMade\") VALUES (?, ?, ?, ?, ?, ?)",
                          when => when.WithParams(objectMissingPartitionKey.GetParameters()))
                      .ThenServerError(ServerError.Invalid, "msg"));

            Assert.Throws<InvalidQueryException>(() => table.Insert(objectMissingPartitionKey).Execute());
        }

        [Test]
        public void LinqInsert_MissingPartitionKey_Async_Test()
        {
            var table = new Table<Movie>(Session, new MappingConfiguration());
            Movie objectMissingPartitionKey = new Movie() { MainActor = "doesntmatter" };

            TestCluster.PrimeFluent(
                b => b.WhenQuery(
                          $"INSERT INTO \"{Movie.TableName}\" (\"director\", \"list\", \"mainGuy\", \"movie_maker\", \"unique_movie_title\", \"yearMade\") VALUES (?, ?, ?, ?, ?, ?)",
                          when => when.WithParams(objectMissingPartitionKey.GetParameters()))
                      .ThenServerError(ServerError.Invalid, "msg"));

            try
            {
                table.Insert(objectMissingPartitionKey).ExecuteAsync().Wait();
            }
            catch (Exception e) // Exception is gathered from the async task
            {
                int maxLayers = 50;
                int layersChecked = 0;
                while (layersChecked < maxLayers && !e.GetType().Equals(typeof(InvalidQueryException)))
                {
                    layersChecked++;
                    e = e.InnerException;
                }
                Assert.IsInstanceOf<InvalidQueryException>(e);
            }
        }

        [Test]
        public void LinqInsert_IfNotExists_Test()
        {
            var nerdMoviesTable = new Table<Movie>(Session, new MappingConfiguration());
            var movie = Movie.GetDefaultMovieList()[0];

            TestCluster.PrimeFluent(
                b => b.WhenQuery(
                          $"INSERT INTO \"{Movie.TableName}\" (\"director\", \"list\", \"mainGuy\", \"movie_maker\", \"unique_movie_title\", \"yearMade\") VALUES (?, ?, ?, ?, ?, ?) IF NOT EXISTS",
                          when => when.WithParams(movie.GetParameters()))
                      .ThenRowsSuccess(Movie.GetEmptyAppliedInfoRowsResult()));

            var appliedInfo = nerdMoviesTable.
                Insert(movie)
                .IfNotExists()
                .Execute();
            Assert.True(appliedInfo.Applied);
            Assert.Null(appliedInfo.Existing);

            var newMovie = Movie.GetDefaultMovieList()[1];
            newMovie.Title = movie.Title;
            newMovie.Director = movie.Director;
            newMovie.MovieMaker = movie.MovieMaker;

            TestCluster.PrimeFluent(
                b => b.WhenQuery(
                          $"INSERT INTO \"{Movie.TableName}\" (\"director\", \"list\", \"mainGuy\", \"movie_maker\", \"unique_movie_title\", \"yearMade\") VALUES (?, ?, ?, ?, ?, ?) IF NOT EXISTS",
                          when => when.WithParams(newMovie.GetParameters()))
                      .ThenRowsSuccess(movie.CreateAppliedInfoRowsResult()));

            appliedInfo = nerdMoviesTable
                .Insert(newMovie)
                .IfNotExists()
                .Execute();

            Assert.False(appliedInfo.Applied);
            Assert.NotNull(appliedInfo.Existing);
            Assert.AreEqual(movie.Year, appliedInfo.Existing.Year);
        }
    }
}