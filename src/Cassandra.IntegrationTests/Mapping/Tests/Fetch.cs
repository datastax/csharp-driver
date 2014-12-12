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
        private readonly Logger _logger = new Logger(typeof(Fetch));
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
            List<string> followersForAuthor = new List<string>() { "follower1", "follower2", "" };
            Author expectedAuthor = new Author
            {
                AuthorId = Guid.NewGuid().ToString(),
                Followers = followersForAuthor,
            };

            cqlClient.Insert(expectedAuthor);
            List<Author> authors = cqlClient.Fetch<Author>("SELECT * from " + table.Name).ToList();
            Assert.AreEqual(1, authors.Count);
            expectedAuthor.AssertEquals(authors[0]);
        }

        /// <summary>
        /// Successfully insert a new record into a table that was created with fluent mapping
        /// </summary>
        [Test]
        public void Fetch_NoArgDefaultsToSelectAll()
        {
            var table = _session.GetTable<ManyDataTypesPoco>(new ManyDataTypesPocoMappingCaseSensitive());
            table.Create();

            var cqlClient = CqlClientConfiguration
                .ForSession(_session)
                .UseIndividualMapping<ManyDataTypesPocoMappingCaseSensitive>()
                .BuildCqlClient();
            List<ManyDataTypesPoco> manyTypesList = new List<ManyDataTypesPoco>();
            for (int i = 0; i < 10; i++)
                manyTypesList.Add(ManyDataTypesPoco.GetRandomInstance());
            foreach (var manyTypesRecord in manyTypesList)
                cqlClient.Insert(manyTypesRecord);

            List<ManyDataTypesPoco> instancesRetrieved = cqlClient.Fetch<ManyDataTypesPoco>().ToList();
            Assert.AreEqual(manyTypesList.Count, instancesRetrieved.Count);

            foreach (var instanceRetrieved in instancesRetrieved)
                ManyDataTypesPoco.AssertListContains(manyTypesList, instanceRetrieved);
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
            List<Author> actualAuthors = cqlClient.Fetch<Author>(cql).ToList();
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
            List<Author> actualAuthors = cqlClient.Fetch<Author>(cql).ToList();
            Assert.AreEqual(1, actualAuthors.Count);
            expectedAuthorFromQuery.AssertEquals(actualAuthors[0]);
        }

        /// <summary>
        /// Page through results from a mapped Fetch request
        /// 
        /// @Jira CSHARP-215 https://datastax-oss.atlassian.net/browse/CSHARP-215
        /// </summary>
        [Test]
        public void Fetch_Lazy()
        {
            Table<Author> table = _session.GetTable<Author>();
            table.Create();

            var cqlClient = CqlClientConfiguration
                .ForSession(_session)
                .UseIndividualMapping<FluentUserMapping>()
                .BuildCqlClient();
            List<Author> expectedAuthors = Author.GetRandomList(100);
            foreach (Author expectedAuthor in expectedAuthors)
                cqlClient.Insert(expectedAuthor);

            Cql cql = new Cql("SELECT * from " + table.Name);
            List<Author> authorsFetchedAndSaved = new List<Author>();
            var authorsFetched = cqlClient.Fetch<Author>(cql).GetEnumerator();
            while (authorsFetched.MoveNext())
                authorsFetchedAndSaved.Add(authorsFetched.Current);

            Assert.AreEqual(expectedAuthors.Count, authorsFetchedAndSaved.Count);
            foreach (var authorFetched in authorsFetchedAndSaved)
            {
                Author.AssertListContains(expectedAuthors, authorFetched);
            }
        }

    }
}
