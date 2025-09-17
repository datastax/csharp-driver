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

using NUnit.Framework;
using Assert = NUnit.Framework.Legacy.ClassicAssert;

namespace Cassandra.IntegrationTests.Linq.LinqMethods
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