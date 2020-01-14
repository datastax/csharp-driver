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

using Cassandra.Data.Linq;
using Cassandra.IntegrationTests.Linq.Structures;
using Cassandra.IntegrationTests.TestBase;
using Cassandra.Mapping;

using NUnit.Framework;

#pragma warning disable 612

namespace Cassandra.IntegrationTests.Linq.LinqMethods
{
    [TestCassandraVersion(2, 0)]
    public class Take : SimulacronTest
    {
        private List<Movie> _movieList = Movie.GetDefaultMovieList();
        private string _uniqueKsName = TestUtils.GetUniqueKeyspaceName();
        private MappingConfiguration _movieMappingConfig;
        private Table<Movie> _movieTable;

        public override void SetUp()
        {
            base.SetUp();
            Session.ChangeKeyspace(_uniqueKsName);

            _movieMappingConfig = new MappingConfiguration();
            _movieMappingConfig.MapperFactory.PocoDataFactory.AddDefinitionDefault(typeof(Movie),
                 () => LinqAttributeBasedTypeDefinition.DetermineAttributes(typeof(Movie)));
            _movieTable = new Table<Movie>(Session, _movieMappingConfig);
        }

        [TestCase(true)]
        [TestCase(false)]
        [Test]
        public void LinqTable_Take_Zero_Sync(bool async)
        {
            TestCluster.PrimeFluent(
                b => b.WhenQuery(
                          "SELECT \"director\", \"list\", \"mainGuy\", \"movie_maker\", \"unique_movie_title\", \"yearMade\" " +
                          $"FROM \"{Movie.TableName}\" ALLOW FILTERING")
                      .ThenRowsSuccess(Movie.CreateRowsResult(_movieList)));

            // Without where clause
            List<Movie> actualMovieList = async 
                ? _movieTable.Take(0).ExecuteAsync().GetAwaiter().GetResult().ToList() 
                : _movieTable.Take(0).Execute().ToList();
            Assert.AreEqual(5, actualMovieList.Count());
        }

        [TestCase(true)]
        [TestCase(false)]
        [Test]
        public void LinqTable_Take_One_Sync(bool async)
        {
            TestCluster.PrimeFluent(
                b => b.WhenQuery(
                          "SELECT \"director\", \"list\", \"mainGuy\", \"movie_maker\", \"unique_movie_title\", \"yearMade\" " +
                          $"FROM \"{Movie.TableName}\" LIMIT ? ALLOW FILTERING",
                          when => when.WithParam(1))
                      .ThenRowsSuccess(_movieList.First().CreateRowsResult()));

            //without where clause
            List<Movie> actualMovieList = async
                ? _movieTable.Take(1).ExecuteAsync().GetAwaiter().GetResult().ToList()
                : _movieTable.Take(1).Execute().ToList();
            Assert.AreEqual(1, actualMovieList.Count());
            Movie.AssertListContains(_movieList, actualMovieList.First());
        }

        [TestCase(true)]
        [TestCase(false)]
        [Test]
        public void LinqTable_Take_Two_Sync(bool async)
        {
            TestCluster.PrimeFluent(
                b => b.WhenQuery(
                          "SELECT \"director\", \"list\", \"mainGuy\", \"movie_maker\", \"unique_movie_title\", \"yearMade\" " +
                          $"FROM \"{Movie.TableName}\" LIMIT ? ALLOW FILTERING",
                          when => when.WithParam(2))
                      .ThenRowsSuccess(Movie.CreateRowsResult(_movieList.Take(2))));

            //without where clause
            List<Movie> actualMovieList = async
                ? _movieTable.Take(2).ExecuteAsync().GetAwaiter().GetResult().ToList()
                : _movieTable.Take(2).Execute().ToList();
            Assert.AreEqual(2, actualMovieList.Count());
            Movie.AssertListContains(_movieList, actualMovieList[0]);
            Movie.AssertListContains(_movieList, actualMovieList[1]);
        }

        [TestCase(true)]
        [TestCase(false)]
        [Test]
        public void LinqTable_Take_UsingWhereClause_Sync(bool async)
        {
            Movie expectedMovie = _movieList.Last();

            TestCluster.PrimeFluent(
                b => b.WhenQuery(
                          "SELECT \"director\", \"list\", \"mainGuy\", \"movie_maker\", \"unique_movie_title\", \"yearMade\" " +
                          $"FROM \"{Movie.TableName}\" WHERE \"director\" = ? AND \"unique_movie_title\" = ? AND \"movie_maker\" = ? LIMIT ? ALLOW FILTERING",
                          when => when.WithParams(expectedMovie.Director, expectedMovie.Title, expectedMovie.MovieMaker, 1))
                      .ThenRowsSuccess(expectedMovie.CreateRowsResult()));

            // Do Take query with where clause
            var actualMovieQuery = _movieTable
                .Where(m => m.Director == expectedMovie.Director && m.Title == expectedMovie.Title && m.MovieMaker == expectedMovie.MovieMaker)
                .Take(1);
                
            var actualMovie = async ? actualMovieQuery.ExecuteAsync().GetAwaiter().GetResult().ToList().First() : actualMovieQuery.Execute().ToList().First();
            Movie.AssertEquals(expectedMovie, actualMovie);
        }

        [TestCase(true)]
        [TestCase(false)]
        [Test]
        public void LinqTable_Take_Sync_CountGreaterThanAvailableRows(bool async)
        {
            int largeNumber = _movieList.Count * 1000;

            TestCluster.PrimeFluent(
                b => b.WhenQuery(
                          "SELECT \"director\", \"list\", \"mainGuy\", \"movie_maker\", \"unique_movie_title\", \"yearMade\" " +
                          $"FROM \"{Movie.TableName}\" LIMIT ? ALLOW FILTERING",
                          when => when.WithParams(largeNumber))
                      .ThenRowsSuccess(Movie.CreateRowsResult(_movieList)));

            // Test
            List<Movie> actualMovieList = async
                ? _movieTable.Take(largeNumber).ExecuteAsync().GetAwaiter().GetResult().ToList()
                : _movieTable.Take(largeNumber).Execute().ToList();
            Assert.AreEqual(_movieList.Count, _movieList.Count());
            foreach (Movie actualMovie in actualMovieList)
                Movie.AssertListContains(_movieList, actualMovie);
        }
    }
}