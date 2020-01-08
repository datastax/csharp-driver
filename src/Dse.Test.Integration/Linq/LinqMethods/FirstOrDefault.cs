//
//  Copyright (C) DataStax, Inc.
//
//  Please see the license for details:
//  http://www.datastax.com/terms/datastax-dse-driver-license-terms
//

using System.Collections.Generic;
using System.Linq;
using Dse.Data.Linq;
using Dse.Test.Integration.Linq.Structures;
using Dse.Test.Integration.TestClusterManagement;
using Dse.Mapping;
using Dse.Test.Integration.TestBase;
using NUnit.Framework;
#pragma warning disable 612

namespace Dse.Test.Integration.Linq.LinqMethods
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
