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

using System;
using System.Collections.Generic;
using System.Linq;
using Cassandra.Data.Linq;
using Cassandra.IntegrationTests.Linq.Tests;
using Cassandra.IntegrationTests.Mapping.Structures;
using Cassandra.IntegrationTests.TestBase;
using Cassandra.Mapping;
using Cassandra.Tests.Mapping.FluentMappings;
using NUnit.Framework;

namespace Cassandra.IntegrationTests.Mapping.Tests
{
    [Category("short")]
    public class Fetch : TestGlobals
    {
        ISession _session = null;
        private readonly Logger _logger = new Logger(typeof(CreateTable));
        string _uniqueKsName;

        [SetUp]
        public void SetupTest()
        {
            _session = TestClusterManager.GetTestCluster(1).Session;
            _uniqueKsName = TestUtils.GetUniqueKeyspaceName();
            _session.CreateKeyspace(_uniqueKsName);
            _session.ChangeKeyspace(_uniqueKsName);
        }

        [TearDown]
        public void TeardownTest()
        {
            _session.DeleteKeyspace(_uniqueKsName);
        }

        /// <summary>
        /// Validate that the mapping mechanism ignores the class variable marked as "Ignore"
        /// </summary>
        [Test]
        public void Fetch_IgnoreAttribute()
        {
            Table<ClassWithIgnoredAttributes> table = _session.GetTable<ClassWithIgnoredAttributes>();
            table.Create();

            var cqlClient = CqlClientConfiguration
                .ForSession(_session)
                .UseIndividualMapping<FluentUserMapping>()
                .BuildCqlClient();
            ClassWithIgnoredAttributes expectedVals = new ClassWithIgnoredAttributes
            {
                SomePartitionKey = Guid.NewGuid().ToString(),
                IgnoredStringAttribute = Guid.NewGuid().ToString(),
            };

            cqlClient.Insert(expectedVals);

            // Get records using mapped object, validate that the value from Cassandra was ignored in favor of the default val
            List<ClassWithIgnoredAttributes> records = cqlClient.Fetch<ClassWithIgnoredAttributes>("SELECT * from " + table.Name);
            Assert.AreEqual(1, records.Count);
            Assert.AreEqual(expectedVals.SomePartitionKey, records[0].SomePartitionKey);
            ClassWithIgnoredAttributes defaultClassWithIgnoredAttributes = new ClassWithIgnoredAttributes();
            Assert.AreEqual(defaultClassWithIgnoredAttributes.IgnoredStringAttribute, records[0].IgnoredStringAttribute);

            // Query for the column that the Linq table create created, verify no value was uploaded to it
            List<Row> rows = _session.Execute("SELECT * from " + table.Name).GetRows().ToList();
            Assert.AreEqual(1, rows.Count);
            Assert.AreEqual(expectedVals.SomePartitionKey, rows[0].GetValue<string>("somepartitionkey"));
            Assert.AreEqual(null, rows[0].GetValue<string>("this_should_be_ignored"));
        }

        /// <summary>
        /// Successfully Fetch mapped records by passing in a static query string
        /// </summary>
        [Test]
        public void Fetch_UsingString_Success()
        {
            Table<Author> table = _session.GetTable<Author>();
            table.Create();

            var cqlClient = CqlClientConfiguration
                .ForSession(_session)
                .UseIndividualMapping<FluentUserMapping>()
                .BuildCqlClient();
            List<string> followersForAuthor = new List<string>()
            {
                "follower1", "follower2", ""
            };
            Author expectedAuthor = new Author
            {
                AuthorId = Guid.NewGuid().ToString(),
                Followers = followersForAuthor,
            };

            cqlClient.Insert(expectedAuthor);
            List<Author> authors = cqlClient.Fetch<Author>("SELECT * from " + table.Name);
            Assert.AreEqual(1, authors.Count);
            expectedAuthor.AssertEquals(authors[0]);
        }

        /// <summary>
        /// Successfully Fetch mapped records by passing in a Cql Object
        /// </summary>
        [Test]
        public void Fetch_UsingCqlObject_Success()
        {
            Table<Author> table = _session.GetTable<Author>();
            table.Create();
            int totalInserts = 10;

            var cqlClient = CqlClientConfiguration
                .ForSession(_session)
                .UseIndividualMapping<FluentUserMapping>()
                .BuildCqlClient();
            List<Author> expectedAuthors = Author.GetRandomList(totalInserts);
            foreach (Author expectedAuthor in expectedAuthors)
                cqlClient.Insert(expectedAuthor);

            Cql cql = new Cql("SELECT * from " + table.Name);
            List<Author> actualAuthors = cqlClient.Fetch<Author>(cql);
            Assert.AreEqual(totalInserts, actualAuthors.Count);
            Author.AssertListsContainTheSame(expectedAuthors, actualAuthors);
        }

        /// <summary>
        /// When a List of strings is uploaded as null, it is downloaded as an empty list
        /// </summary>
        [Test]
        public void Fetch_ListUploadedAsNull()
        {
            Table<Author> table = _session.GetTable<Author>();
            table.Create();
            var cqlClient = CqlClientConfiguration
                .ForSession(_session)
                .UseIndividualMapping<FluentUserMapping>()
                .BuildCqlClient();
            Author expectedAuthor = Author.GetRandom();
            expectedAuthor.Followers = null;
            cqlClient.Insert(expectedAuthor);

            // Assert values from cql query
            Author expectedAuthorFromQuery = new Author
            {
                Followers = new List<string>(), // not null
                AuthorId = expectedAuthor.AuthorId,
            };

            Cql cql = new Cql("SELECT * from " + table.Name);
            List<Author> actualAuthors = cqlClient.Fetch<Author>(cql);
            Assert.AreEqual(1, actualAuthors.Count);
            expectedAuthorFromQuery.AssertEquals(actualAuthors[0]);
        }

        /// <summary>
        /// Page through results according to the page setting in CQL object used for th Fetch request
        /// </summary>
        [Test, NUnit.Framework.Ignore("TBD")]
        public void Fetch_Lazy()
        {
            Table<Author> table = _session.GetTable<Author>();
            table.Create();
            int totalAuthorsToInsert = 100;
            int pageSize = 10;
            Assert.AreEqual(0, totalAuthorsToInsert%pageSize);
            int expectedNumberOfPages = totalAuthorsToInsert/pageSize;
            int actualNumberOfPages = 0;

            var cqlClient = CqlClientConfiguration
                .ForSession(_session)
                .UseIndividualMapping<FluentUserMapping>()
                .BuildCqlClient();
            List<Author> expectedAuthors = Author.GetRandomList(totalAuthorsToInsert);
            foreach (Author expectedAuthor in expectedAuthors)
                cqlClient.Insert(expectedAuthor);

            Cql cql = new Cql("SELECT * from " + table.Name);
            cql.QueryOptions.SetPageSize(pageSize);

            List<Author> allAuthorsFetched = new List<Author>();
            List<Author> authorsFetchedSinglePage = cqlClient.Fetch<Author>(cql);
            while (authorsFetchedSinglePage.Count > 0)
            {
                actualNumberOfPages++;
                allAuthorsFetched.AddRange(authorsFetchedSinglePage);
                authorsFetchedSinglePage = cqlClient.Fetch<Author>(cql);
            }

            Assert.AreEqual(expectedAuthors.Count, allAuthorsFetched.Count);
            foreach (var authorFetched in allAuthorsFetched)
            {
                Author.AssertListContains(expectedAuthors, authorFetched);
            }
            Assert.AreEqual(expectedNumberOfPages, actualNumberOfPages);
        }

        /////////////////////////////////////////
        /// Private test classes
        /////////////////////////////////////////

        [Table("classwithignoredattributes")]
        private class ClassWithIgnoredAttributes
        {
            [PartitionKey]
            [Cassandra.Data.Linq.Column("somepartitionkey")]
            public string SomePartitionKey = "somePartitionKey";

            [Cassandra.Mapping.IgnoreAttribute]
            [Cassandra.Data.Linq.Column("this_should_be_ignored")]
            public string IgnoredStringAttribute = "someIgnoredString";
        }

    }
}
