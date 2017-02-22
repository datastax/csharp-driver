//
//  Copyright (C) 2017 DataStax, Inc.
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
    [Category("short")]
    public class TableSelectDefault: SharedClusterTest
    {
        private ISession _session;
        private List<Movie> _movieList = Movie.GetDefaultMovieList();
        private string _uniqueKsName = TestUtils.GetUniqueKeyspaceName();

        public override void OneTimeSetUp()
        {
            base.OneTimeSetUp();
            _session = Session;
            _session.CreateKeyspace(_uniqueKsName);
            _session.ChangeKeyspace(_uniqueKsName);

            // drop table if exists, re-create
            var table = _session.GetTable<Movie>();
            table.Create();

            //Insert some data
            foreach (var movie in _movieList)
                table.Insert(movie).Execute();
        }

        [Test]
        public void LinqTable_Sync()
        {
            var table = _session.GetTable<Movie>();
            var movies = table.Execute().ToArray();
            Assert.AreEqual(_movieList.Count, movies.Length);
        }

        [Test]
        public void LinqTable_Async()
        {
            // insert new row
            var table = _session.GetTable<Movie>();
            var movies = table.ExecuteAsync().Result.ToArray();
            Assert.AreEqual(_movieList.Count, movies.Length);
        }
    }
}
