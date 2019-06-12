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
using Cassandra.Mapping.Attributes;
using NUnit.Framework;
#pragma warning disable 612

namespace Cassandra.IntegrationTests.Linq.LinqMethods
{
    [Category("short"), TestCassandraVersion(2, 0)]
    public class Take : SharedClusterTest
    {
        ISession _session = null;
        private List<Movie> _movieList = Movie.GetDefaultMovieList();
        string _uniqueKsName = TestUtils.GetUniqueKeyspaceName();
        private MappingConfiguration _movieMappingConfig;
        private Table<Movie> _movieTable;

        public override void OneTimeSetUp()
        {
            base.OneTimeSetUp();
            _session = Session;
            _session.CreateKeyspace(_uniqueKsName);
            _session.ChangeKeyspace(_uniqueKsName);

            // drop table if exists, re-create
            _movieMappingConfig = new MappingConfiguration();
            _movieMappingConfig.MapperFactory.PocoDataFactory.AddDefinitionDefault(typeof(Movie),
                 () => LinqAttributeBasedTypeDefinition.DetermineAttributes(typeof(Movie)));
            _movieTable = new Table<Movie>(_session, _movieMappingConfig);
            _movieTable.Create();

            //Insert some data
            foreach (var movie in _movieList)
                _movieTable.Insert(movie).Execute();
        }

        [Test]
        public void LinqTable_Take_Zero_Sync()
        {
            // Without where clause
            List<Movie> actualMovieList = _movieTable.Take(0).Execute().ToList();
            Assert.AreEqual(5, actualMovieList.Count());
        }

        [Test]
        public void LinqTable_Take_One_Sync()
        {
            //without where clause
            List<Movie> actualMovieList = _movieTable.Take(1).Execute().ToList();
            Assert.AreEqual(1, actualMovieList.Count());
            Movie.AssertListContains(_movieList, actualMovieList.First());
        }

        [Test]
        public void LinqTable_Take_Two_Sync()
        {
            //without where clause
            List<Movie> actualMovieList = _movieTable.Take(2).Execute().ToList();
            Assert.AreEqual(2, actualMovieList.Count());
            Movie.AssertListContains(_movieList, actualMovieList[0]);
            Movie.AssertListContains(_movieList, actualMovieList[1]);
        }

        [Test]
        public void LinqTable_Take_UsingWhereClause_Sync()
        {
            Movie expectedMovie = _movieList.Last();

            // Do Take query with where clause
            var actualMovie = _movieTable
                .Where(m => m.Director == expectedMovie.Director && m.Title == expectedMovie.Title && m.MovieMaker == expectedMovie.MovieMaker)
                .Take(1).Execute().ToList().First();
            Movie.AssertEquals(expectedMovie, actualMovie);

        }

        [Test]
        public void LinqTable_Take_Sync_CountGreaterThanAvailableRows()
        {
            int largeNumber = _movieList.Count * 1000;

            // Test
            List<Movie> actualMovieList = _movieTable.Take(largeNumber).Execute().ToList();
            Assert.AreEqual(_movieList.Count, _movieList.Count());
            foreach (Movie actualMovie in actualMovieList)
                Movie.AssertListContains(_movieList, actualMovie);
        }

        [Test]
        public void LinqTable_Take_Zero_Async()
        {
            //without where clause
            List<Movie> actualMovieList = _movieTable.Take(0).ExecuteAsync().Result.ToList();
            Assert.AreEqual(5, actualMovieList.Count());
        }

        [Test]
        public void LinqTable_Take_One_Async()
        {
            //without where clause
            List<Movie> actualMovieList = _movieTable.Take(1).ExecuteAsync().Result.ToList();
            Assert.AreEqual(1, actualMovieList.Count());
            Movie.AssertListContains(_movieList, actualMovieList.First());
        }

        [Test]
        public void LinqTable_Take_Two_Async()
        {
            //without where clause
            List<Movie> actualMovieList = _movieTable.Take(2).ExecuteAsync().Result.ToList();
            Assert.AreEqual(2, actualMovieList.Count());
            Movie.AssertListContains(_movieList, actualMovieList[0]);
            Movie.AssertListContains(_movieList, actualMovieList[1]);
        }

        [Test]
        public void LinqTable_Take_UsingWhereClause_Async()
        {
            Movie expectedMovie = _movieList.Last();

            //with where clause
            var actualMovie = _movieTable
                .Where(m => m.Director == expectedMovie.Director && m.Title == expectedMovie.Title && m.MovieMaker == expectedMovie.MovieMaker)
                .Take(1).ExecuteAsync().Result.ToList().First();
            Movie.AssertEquals(expectedMovie, actualMovie);

        }

        [Test]
        public void LinqTable_Take_Async_CountGreaterThanAvailableRows()
        {
            int largeNumber = _movieList.Count * 1000;

            // Test
            List<Movie> actualMovieList = _movieTable.Take(largeNumber).ExecuteAsync().Result.ToList();
            Assert.AreEqual(_movieList.Count, _movieList.Count());
            foreach (Movie actualMovie in actualMovieList)
                Movie.AssertListContains(_movieList, actualMovie);
        }


    }
}
