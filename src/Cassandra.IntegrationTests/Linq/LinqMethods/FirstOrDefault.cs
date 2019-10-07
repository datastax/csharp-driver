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
    [Category("short"), Category("realcluster"), TestCassandraVersion(2, 0)]
    public class FirstOrDefault : SharedClusterTest
    {
        private ISession _session;
        private List<Movie> _movieList = Movie.GetDefaultMovieList();
        string _uniqueKsName = TestUtils.GetUniqueKeyspaceName();
        private Table<Movie> _movieTable;

        public override void OneTimeSetUp()
        {
            base.OneTimeSetUp();
            _session = Session;
            _session.CreateKeyspace(_uniqueKsName);
            _session.ChangeKeyspace(_uniqueKsName);

            // drop table if exists, re-create
            MappingConfiguration movieMappingConfig = new MappingConfiguration();
            movieMappingConfig.MapperFactory.PocoDataFactory.AddDefinitionDefault(typeof(Movie),
                 () => LinqAttributeBasedTypeDefinition.DetermineAttributes(typeof(Movie)));
            _movieTable = new Table<Movie>(_session, movieMappingConfig);
            _movieTable.Create();

            //Insert some data
            foreach (var movie in _movieList)
                _movieTable.Insert(movie).Execute();
        }

        [Test]
        public void LinqFirstOrDefault_Sync()
        {
            var expectedMovie = _movieList.First();

            // Test
            var first =
                _movieTable.FirstOrDefault(
                    m => m.Director == expectedMovie.Director && m.Title == expectedMovie.Title && m.MovieMaker == expectedMovie.MovieMaker).Execute();
            Assert.IsNotNull(first);
            Assert.AreEqual(expectedMovie.MovieMaker, first.MovieMaker);
        }

        [Test]
        public void LinqFirstOrDefault_Sync_NoSuchRecord()
        {
            var first = _movieTable.FirstOrDefault(m => m.Director == "non_existant_" + Randomm.RandomAlphaNum(10)).Execute();
            Assert.IsNull(first);
        }

        [Test]
        public void LinqFirstOrDefault_Async()
        {
            // Setup
            _movieTable = new Table<Movie>(_session, new MappingConfiguration());
            var expectedMovie = _movieList.Last();

            // Test
            var actualMovie =
                _movieTable.FirstOrDefault(
                    m => m.Director == expectedMovie.Director && m.Title == expectedMovie.Title && m.MovieMaker == expectedMovie.MovieMaker)
                     .ExecuteAsync()
                     .Result;
            Assert.IsNotNull(actualMovie);
            Movie.AssertEquals(expectedMovie, actualMovie);
        }

        [Test]
        public void LinqFirstOrDefault_Async_NoSuchRecord()
        {
            var table = new Table<Movie>(_session, new MappingConfiguration());
            var first = table.FirstOrDefault(m => m.Director == "non_existant_" + Randomm.RandomAlphaNum(10)).ExecuteAsync().Result;
            Assert.IsNull(first);
        }
    }
}
