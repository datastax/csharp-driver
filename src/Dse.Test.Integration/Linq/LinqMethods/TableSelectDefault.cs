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
using NUnit.Framework;

namespace Dse.Test.Integration.Linq.LinqMethods
{
    public class TableSelectDefault : SimulacronTest
    {
        private static readonly string UniqueKsName = TestUtils.GetUniqueKeyspaceName();

        private readonly List<Movie> _movieList = Movie.GetDefaultMovieList();

        public TableSelectDefault() : base(keyspace: UniqueKsName)
        {
        }

        [TestCase(true)]
        [TestCase(false)]
        [Test]
        public void LinqTable(bool async)
        {
            TestCluster.PrimeFluent(
                b => b.WhenQuery(
                          "SELECT \"director\", \"list\", \"mainGuy\", \"movie_maker\", \"unique_movie_title\", \"yearMade\" " +
                          $"FROM \"{Movie.TableName}\" ALLOW FILTERING")
                      .ThenRowsSuccess(Movie.CreateRowsResult(_movieList)));
            var table = Session.GetTable<Movie>();
            var movies = async ? table.ExecuteAsync().GetAwaiter().GetResult().ToArray() : table.Execute().ToArray();
            Assert.AreEqual(_movieList.Count, movies.Length);
        }
    }
}