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
    public class FirstOrDefault : SimulacronTest
    {
        private readonly List<Movie> _movieList = Movie.GetDefaultMovieList();
        private Table<Movie> _movieTable;
        
        public override void SetUp()
        {
            base.SetUp();
            
            MappingConfiguration movieMappingConfig = new MappingConfiguration();
            movieMappingConfig.MapperFactory.PocoDataFactory.AddDefinitionDefault(typeof(Movie),
                () => LinqAttributeBasedTypeDefinition.DetermineAttributes(typeof(Movie)));
            _movieTable = new Table<Movie>(Session, movieMappingConfig);
        }

        [TestCase(true)]
        [TestCase(false)]
        [Test]
        public void LinqFirstOrDefault(bool async)
        {
            var expectedMovie = _movieList.First();
            
            TestCluster.PrimeFluent(
                b => b.WhenQuery(
                          "SELECT \"director\", \"list\", \"mainGuy\", \"movie_maker\", \"unique_movie_title\", \"yearMade\" " +
                          $"FROM \"{Movie.TableName}\" WHERE \"director\" = ? AND \"unique_movie_title\" = ? AND \"movie_maker\" = ? LIMIT ? ALLOW FILTERING",
                          rows => rows.WithParams(expectedMovie.Director, expectedMovie.Title, expectedMovie.MovieMaker, 1))
                      .ThenRowsSuccess(expectedMovie.CreateRowsResult()));

            // Test
            var firstQuery =
                _movieTable.FirstOrDefault(
                    m => m.Director == expectedMovie.Director && m.Title == expectedMovie.Title && m.MovieMaker == expectedMovie.MovieMaker);
            var first = async ? firstQuery.ExecuteAsync().Result : firstQuery.Execute();
            Assert.IsNotNull(first);
            Assert.AreEqual(expectedMovie.MovieMaker, first.MovieMaker);
        }
        
        [TestCase(true)]
        [TestCase(false)]
        [Test]
        public void LinqFirstOrDefault_NoSuchRecord(bool async)
        {
            var randomStr = ConstantReturningHelper.FromObj(Randomm.RandomAlphaNum(10));
            TestCluster.PrimeFluent(
                b => b.WhenQuery(
                          "SELECT \"director\", \"list\", \"mainGuy\", \"movie_maker\", \"unique_movie_title\", \"yearMade\" " +
                          $"FROM \"{Movie.TableName}\" WHERE \"director\" = ? LIMIT ? ALLOW FILTERING",
                          rows => rows.WithParams("non_existant_" + randomStr.Get(), 1))
                      .ThenRowsSuccess(Movie.GetColumns()));

            var firstQuery = _movieTable.FirstOrDefault(m => m.Director == "non_existant_" + randomStr.Get());
            var first = async ? firstQuery.ExecuteAsync().Result : firstQuery.Execute();
            Assert.IsNull(first);
        }
    }
}
