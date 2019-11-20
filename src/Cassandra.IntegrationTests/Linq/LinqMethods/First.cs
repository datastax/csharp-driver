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
using System.Threading;
using Cassandra.Data.Linq;
using Cassandra.IntegrationTests.Linq.Structures;
using Cassandra.IntegrationTests.TestBase;
using Cassandra.IntegrationTests.TestClusterManagement;
using Cassandra.Mapping;
using NUnit.Framework;
using NUnit.Framework.Constraints;
#pragma warning disable 612

namespace Cassandra.IntegrationTests.Linq.LinqMethods
{
    [Category("short"), Category("realcluster"), TestCassandraVersion(2,0)]
    public class First : SharedClusterTest
    {
        ISession _session = null;
        private List<Movie> _movieList = Movie.GetDefaultMovieList();
        private string _uniqueKsName = TestUtils.GetUniqueKeyspaceName();
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

            // Wait for data to be query-able
            DateTime futureDateTime = DateTime.Now.AddSeconds(2); // it should not take very long for these records to become available for querying!
            while (DateTime.Now < futureDateTime && _movieTable.Count().Execute() < _movieList.Count)
                Thread.Sleep(200);
            Assert.AreEqual(_movieList.Count(), _movieTable.Count().Execute(), "Setup failure: Expected number of records are not query-able");
        }

        [Test]
        public void First_ExecuteAsync()
        {
            var expectedMovie = _movieList.First();

            var actualMovie = _movieTable.First(m => m.Title == expectedMovie.Title && m.MovieMaker == expectedMovie.MovieMaker).ExecuteAsync().Result;
            Movie.AssertEquals(expectedMovie, actualMovie);
        }

        [Test]
        public void First_ExecuteSync()
        {
            var expectedMovie = _movieList.First();

            var actualMovie = _movieTable.First(m => m.Title == expectedMovie.Title && m.MovieMaker == expectedMovie.MovieMaker).Execute();
            Movie.AssertEquals(expectedMovie, actualMovie);
        }

        [Test]
        public void First_NoSuchRecord()
        {
            Movie existingMovie = _movieList.Last();
            string randomStr = "somethingrandom_" + Randomm.RandomAlphaNum(10);

            Movie foundMovie = _movieTable.First(m => m.Title == existingMovie.Title && m.MovieMaker == randomStr).Execute();
            Assert.Null(foundMovie);
        }

        ///////////////////////////////////////////////
        /// Exceptions
        ///////////////////////////////////////////////

        [Test]
        public void First_NoTranslationFromLinqToCql()
        {
            //No translation in CQL
            Assert.Throws<CqlLinqNotSupportedException>(() => _movieTable.First(m => m.Year is int).Execute());
        }

        [Test]
        [TestCassandraVersion(3, 0, Comparison.LessThan)]
        public void First_NoPartitionKey()
        {
            //No partition key in Query
            Assert.Throws<InvalidQueryException>(() => _movieTable.First(m => m.Year == 100).Execute());
            Assert.Throws<InvalidQueryException>(() => _movieTable.First(m => m.MainActor == null).Execute());
        }

        [Test]
        public void First_With_Serial_ConsistencyLevel()
        {
            Assert.DoesNotThrow(() => _movieTable.First().SetConsistencyLevel(ConsistencyLevel.Serial).Execute());
        }

        /// <summary>
        /// Test if driver throws exception when query without all partition keys.
        /// Since Cassandra 3.10 it is not expected to throw InvalidException.
        ///
        /// @expected_errors InvalidQueryException
        /// @jira_ticket CASSANDRA-11031
        /// @test_assumptions
        ///     - Cassandra version less than 3.10
        /// </summary>
        [Test]
        [TestCassandraVersion(3, 9, Comparison.LessThan)]
        public void First_MissingPartitionKey()
        {
            var randomStr = "somethingrandom_" + Randomm.RandomAlphaNum(10);

            try
            {
                _movieTable.First(m => m.Title == randomStr).Execute();
                Assert.Fail("expected exception was not thrown!");
            }
            catch (InvalidQueryException e)
            {
                string expectedErrMsg = "Partition key part(s:)? movie_maker must be restricted (since preceding part is|as other parts are)";
                StringAssert.IsMatch(expectedErrMsg, e.Message);
            }
        }
    }
}
