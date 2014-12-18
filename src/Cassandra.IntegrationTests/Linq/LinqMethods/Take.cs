//
//      Copyright (C) 2012-2014 DataStax Inc.
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
using NUnit.Framework;

namespace Cassandra.IntegrationTests.Linq.LinqMethods
{
    [Category("short")]
    public class Take : TestGlobals
    {
        ISession _session = null;
        private List<Movie> _movieList = Movie.GetDefaultMovieList();
        string _uniqueKsName = TestUtils.GetUniqueKeyspaceName();
        
        [SetUp]
        public void SetupTest()
        {
            _session = TestClusterManager.GetTestCluster(1).Session;
            _session.CreateKeyspace(_uniqueKsName);
            _session.ChangeKeyspace(_uniqueKsName);

            // drop table if exists, re-create
            var table = _session.GetTable<Movie>();
            table.Create();

            //Insert some data
            foreach (var movie in _movieList)
                table.Insert(movie).Execute();
        }

        [TearDown]
        public void TeardownTest()
        {
            _session.DeleteKeyspace(_uniqueKsName);
        }

        [Test]
        public void LinqTable_Take_Zero_Sync()
        {
            // Setup
            var table = _session.GetTable<Movie>();

            // Without where clause
            List<Movie> actualMovieList = table.Take(0).Execute().ToList();
            Assert.AreEqual(5, actualMovieList.Count());
        }

        [Test]
        public void LinqTable_Take_One_Sync()
        {
            // Setup
            var table = _session.GetTable<Movie>();

            //without where clause
            List<Movie> actualMovieList = table.Take(1).Execute().ToList();
            Assert.AreEqual(1, actualMovieList.Count());
            Movie.AssertListContains(_movieList, actualMovieList.First());
        }

        [Test]
        public void LinqTable_Take_Two_Sync()
        {
            // Setup
            var table = _session.GetTable<Movie>();

            //without where clause
            List<Movie> actualMovieList = table.Take(2).Execute().ToList();
            Assert.AreEqual(2, actualMovieList.Count());
            Movie.AssertListContains(_movieList, actualMovieList[0]);
            Movie.AssertListContains(_movieList, actualMovieList[1]);
        }

        [Test]
        public void LinqTable_Take_UsingWhereClause_Sync()
        {
            // Setup
            var table = _session.GetTable<Movie>();
            Movie expectedMovie = _movieList.Last();

            //with where clause
            var actualMovie = table
                .Where(m => m.Director == expectedMovie.Director && m.Title == expectedMovie.Title && m.MovieMaker == expectedMovie.MovieMaker)
                .Take(1).Execute().ToList().First();
            Movie.AssertEquals(expectedMovie, actualMovie);

        }

        [Test]
        public void LinqTable_Take_Sync_CountGreaterThanAvailableRows()
        {
            // Setup
            var table = _session.GetTable<Movie>();
            int largeNumber = _movieList.Count * 1000;

            // Test
            List<Movie> actualMovieList = table.Take(largeNumber).Execute().ToList();
            Assert.AreEqual(_movieList.Count, _movieList.Count());
            foreach (Movie actualMovie in actualMovieList)
                Movie.AssertListContains(_movieList, actualMovie);
        }

        [Test]
        public void LinqTable_Take_Zero_Async()
        {
            // Setup
            var table = _session.GetTable<Movie>();

            //without where clause
            List<Movie> actualMovieList = table.Take(0).ExecuteAsync().Result.ToList();

            Assert.AreEqual(5, actualMovieList.Count());
        }

        [Test]
        public void LinqTable_Take_One_Async()
        {
            // Setup
            var table = _session.GetTable<Movie>();

            //without where clause
            List<Movie> actualMovieList = table.Take(1).ExecuteAsync().Result.ToList();
            Assert.AreEqual(1, actualMovieList.Count());
            Movie.AssertListContains(_movieList, actualMovieList.First());
        }

        [Test]
        public void LinqTable_Take_Two_Async()
        {
            // Setup
            var table = _session.GetTable<Movie>();

            //without where clause
            List<Movie> actualMovieList = table.Take(2).ExecuteAsync().Result.ToList();
            Assert.AreEqual(2, actualMovieList.Count());
            Movie.AssertListContains(_movieList, actualMovieList[0]);
            Movie.AssertListContains(_movieList, actualMovieList[1]);
        }

        [Test]
        public void LinqTable_Take_UsingWhereClause_Async()
        {
            // Setup
            var table = _session.GetTable<Movie>();
            Movie expectedMovie = _movieList.Last();

            //with where clause
            var actualMovie = table
                .Where(m => m.Director == expectedMovie.Director && m.Title == expectedMovie.Title && m.MovieMaker == expectedMovie.MovieMaker)
                .Take(1).ExecuteAsync().Result.ToList().First();
            Movie.AssertEquals(expectedMovie, actualMovie);

        }

        [Test]
        public void LinqTable_Take_Async_CountGreaterThanAvailableRows()
        {
            // Setup
            var table = _session.GetTable<Movie>();
            int largeNumber = _movieList.Count * 1000;

            // Test
            List<Movie> actualMovieList = table.Take(largeNumber).ExecuteAsync().Result.ToList();
            Assert.AreEqual(_movieList.Count, _movieList.Count());
            foreach (Movie actualMovie in actualMovieList)
                Movie.AssertListContains(_movieList, actualMovie);
        }


    }
}
