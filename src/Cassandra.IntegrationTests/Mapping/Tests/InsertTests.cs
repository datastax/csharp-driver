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
using System.Threading;
using Cassandra.Data.Linq;
using Cassandra.IntegrationTests.Mapping.Structures;
using Cassandra.IntegrationTests.TestBase;
using Cassandra.IntegrationTests.TestClusterManagement;
using Cassandra.Mapping;
using Cassandra.Serialization;
using Cassandra.Tests.Mapping.Pocos;
using NUnit.Framework;
using HairColor = Cassandra.Tests.Mapping.Pocos.HairColor;
#pragma warning disable 169

namespace Cassandra.IntegrationTests.Mapping.Tests
{
    [Category("short")]
    public class InsertTests : SharedClusterTest
    {
        private ISession _session;
        private string _uniqueKsName;

        public override void OneTimeSetUp()
        {
            base.OneTimeSetUp();
            _session = Session;
        }

        [SetUp]
        public void TestSetup()
        {
            _uniqueKsName = TestUtils.GetUniqueKeyspaceName().ToLowerInvariant();
            _session.CreateKeyspace(_uniqueKsName);
            _session.ChangeKeyspace(_uniqueKsName);
        }

        /// <summary>
        /// Successfully insert a new record into a table that was created with fluent mapping
        /// </summary>
        [Test]
        public void Insert_Sync()
        {
            // Setup
            var mappingConfig = new MappingConfiguration().Define(new Map<lowercaseclassnamepklowercase>()
                .TableName(TestUtils.GetUniqueTableName().ToLowerInvariant()).PartitionKey(c => c.somepartitionkey).CaseSensitive());
            var table = new Table<lowercaseclassnamepklowercase>(_session, mappingConfig);
            Assert.AreEqual(table.Name, table.Name.ToLower());
            table.Create();

            // Insert using Mapper.Insert
            lowercaseclassnamepklowercase privateClassInstance = new lowercaseclassnamepklowercase();
            var mapper = new Mapper(_session, mappingConfig);
            mapper.Insert(privateClassInstance);
            List<lowercaseclassnamepklowercase> instancesQueried = mapper.Fetch<lowercaseclassnamepklowercase>().ToList();
            Assert.AreEqual(1, instancesQueried.Count);
            lowercaseclassnamepklowercase defaultInstance = new lowercaseclassnamepklowercase();
            Assert.AreEqual(defaultInstance.somepartitionkey, instancesQueried[0].somepartitionkey);
        }
        
        /// <summary>
        /// Successfully insert a new record into a table that was created with fluent mapping, inserting asynchronously
        /// </summary>
        [Test]
        public void Insert_Async()
        {
            // Setup
            var mappingConfig = new MappingConfiguration().Define(new Map<lowercaseclassnamepklowercase>().PartitionKey(c => c.somepartitionkey).CaseSensitive());
            var table = new Table<lowercaseclassnamepklowercase>(_session, mappingConfig);
            Assert.AreEqual(table.Name, table.Name.ToLower());
            table.Create();

            // Insert using Mapper.Insert
            lowercaseclassnamepklowercase privateClassInstance = new lowercaseclassnamepklowercase();
            var mapper = new Mapper(_session, mappingConfig);
            mapper.InsertAsync(privateClassInstance).Wait();

            // Validate data in C*
            List<lowercaseclassnamepklowercase> instancesQueried = mapper.Fetch<lowercaseclassnamepklowercase>().ToList();
            DateTime futureDateTime = DateTime.Now.AddSeconds(2);
            while (instancesQueried.Count < 1 && futureDateTime > DateTime.Now)
            {
                instancesQueried = mapper.Fetch<lowercaseclassnamepklowercase>().ToList();
            }
            Assert.AreEqual(1, instancesQueried.Count);
            lowercaseclassnamepklowercase defaultInstance = new lowercaseclassnamepklowercase();
            Assert.AreEqual(defaultInstance.somepartitionkey, instancesQueried[0].somepartitionkey);
        }

        /// <summary>
        /// Successfully insert a new record into a table that was created with fluent mapping,
        /// including every acceptable consistency level
        /// </summary>
        [Test]
        public void Insert_WithConsistency_Success()
        {
            // Setup
            var mappingConfig = new MappingConfiguration().Define(new Map<lowercaseclassnamepklowercase>()
                .TableName(TestUtils.GetUniqueTableName().ToLowerInvariant()).PartitionKey(c => c.somepartitionkey).CaseSensitive());
            var table = new Table<lowercaseclassnamepklowercase>(_session, mappingConfig);
            table.Create();
            var mapper = new Mapper(_session, mappingConfig);

            // Insert the data
            var consistencyLevels = new []
            {
                ConsistencyLevel.All,
                ConsistencyLevel.Any,
                ConsistencyLevel.EachQuorum,
                ConsistencyLevel.LocalOne,
                ConsistencyLevel.LocalQuorum,
                ConsistencyLevel.Quorum,
            };
            foreach (var consistencyLevel in consistencyLevels)
            {
                lowercaseclassnamepklowercase pocoInstance = new lowercaseclassnamepklowercase();
                pocoInstance.somepartitionkey = Guid.NewGuid().ToString();
                mapper.Insert(pocoInstance, new CqlQueryOptions().SetConsistencyLevel(consistencyLevel));

                // Assert final state of C* data
                string cql = "Select * from " + table.Name + " where somepartitionkey ='" + pocoInstance.somepartitionkey + "'";
                List<lowercaseclassnamepklowercase> instancesQueried = mapper.Fetch<lowercaseclassnamepklowercase>(cql).ToList();
                DateTime futureDateTime = DateTime.Now.AddSeconds(2);
                while (instancesQueried.Count < 1 && futureDateTime > DateTime.Now)
                {
                    instancesQueried = mapper.Fetch<lowercaseclassnamepklowercase>(cql).ToList();
                }
                Assert.AreEqual(1, instancesQueried.Count, "Unexpected failure for consistency level: " + consistencyLevel);
                Assert.AreEqual(pocoInstance.somepartitionkey, instancesQueried[0].somepartitionkey);
            }
        }

        /// <summary>
        /// Successfully insert a new record into a table that was created with fluent mapping,
        /// including a consistency level of 'Serial'
        /// Validate expected error message
        /// </summary>
        [Test]
        public void Insert_WithConsistency_Serial()
        {
            // Setup
            var mappingConfig = new MappingConfiguration().Define(new Map<lowercaseclassnamepklowercase>()
                .TableName(TestUtils.GetUniqueTableName().ToLowerInvariant()).PartitionKey(c => c.somepartitionkey).CaseSensitive());
            var table = new Table<lowercaseclassnamepklowercase>(_session, mappingConfig);
            table.Create();

            // Insert the data
            var mapper = new Mapper(_session, mappingConfig);
            lowercaseclassnamepklowercase pocoInstance = new lowercaseclassnamepklowercase();

            // No conditional INSERT, it must fail with serial consistency
            Assert.Throws<InvalidQueryException>(
                () => mapper.Insert(pocoInstance, new CqlQueryOptions().SetConsistencyLevel(ConsistencyLevel.Serial)));
        }

        /// <summary>
        /// Successfully insert a new record into a table that was created with fluent mapping,
        /// including consistency levels that will cause the request to fail silently.
        /// </summary>
        [Test]
        public void Insert_WithConsistencyLevel_Fail()
        {
            // Setup
            var tableName = TestUtils.GetUniqueTableName().ToLowerInvariant();
            var mappingConfig =
                new MappingConfiguration().Define(new Map<lowercaseclassnamepklowercase>().PartitionKey(c => c.somepartitionkey).TableName(tableName).CaseSensitive());
            var table = new Table<lowercaseclassnamepklowercase>(_session, mappingConfig);
            table.Create();

            // Insert the data
            var consistencyLevels = new []
            {
                ConsistencyLevel.Three,
                ConsistencyLevel.Two
            };
            var mapper = new Mapper(_session, mappingConfig);
            foreach (var consistencyLevel in consistencyLevels)
            {
                lowercaseclassnamepklowercase privateClassInstance = new lowercaseclassnamepklowercase();
                Assert.Throws<UnavailableException>(() => mapper.Insert(privateClassInstance, new CqlQueryOptions().SetConsistencyLevel(consistencyLevel)));
            }
        }

        /// <summary>
        /// Successfully insert a new record into a table that was created with fluent mapping, inserting asynchronously
        /// including a consistency level that one greater than the current node count
        /// </summary>
        [Test]
        public void Insert_Async_WithConsistency_OneMoreCopyThanNodeCount()
        {
            // Setup
            var mappingConfig =
                new MappingConfiguration().Define(new Map<lowercaseclassnamepklowercase>().PartitionKey(c => c.somepartitionkey).CaseSensitive());

            // Insert the data
            lowercaseclassnamepklowercase privateClassInstance = new lowercaseclassnamepklowercase();
            var mapper = new Mapper(_session, mappingConfig);
            Assert.Throws<AggregateException>(() => mapper.InsertAsync(privateClassInstance, new CqlQueryOptions().SetConsistencyLevel(ConsistencyLevel.Two)).Wait());
        }

        /// <summary>
        /// Successfully insert a new record into a table that was created with fluent mapping, using Mapper.Insert
        /// </summary>
        [Test]
        public void Insert_WithMapperInsert_TwoPartitionKeys_OnlyOne()
        {
            // Setup
            var mappingConfig = new MappingConfiguration().Define(new Map<ClassWithTwoPartitionKeys>()
                .TableName(typeof (ClassWithTwoPartitionKeys).Name).CaseSensitive()
                .PartitionKey(new string[] {"PartitionKey1", "PartitionKey2" }).CaseSensitive()
                );
            var table = new Table<ClassWithTwoPartitionKeys>(_session, mappingConfig);
            table.Create();

            // Insert the data
            ClassWithTwoPartitionKeys defaultInstance = new ClassWithTwoPartitionKeys();
            ClassWithTwoPartitionKeys instance = new ClassWithTwoPartitionKeys();
            var mapper = new Mapper(_session, mappingConfig);
            mapper.Insert(instance);

            List<ClassWithTwoPartitionKeys> instancesRetrieved = new List<ClassWithTwoPartitionKeys>();
            DateTime futureDateTime = DateTime.Now.AddSeconds(5);
            while (instancesRetrieved.Count < 1 && DateTime.Now < futureDateTime)
                instancesRetrieved = mapper.Fetch<ClassWithTwoPartitionKeys>("SELECT * from \"" + table.Name + "\"").ToList();
            Assert.AreEqual(1, instancesRetrieved.Count);
            Assert.AreEqual(defaultInstance.PartitionKey1, instancesRetrieved[0].PartitionKey1);
            Assert.AreEqual(defaultInstance.PartitionKey2, instancesRetrieved[0].PartitionKey2);
            instancesRetrieved.Clear();

            futureDateTime = DateTime.Now.AddSeconds(5);
            string cqlSelect = "SELECT * from \"" + table.Name + "\" where \"PartitionKey1\" = '" + instance.PartitionKey1 + "' and \"PartitionKey2\" = '" + instance.PartitionKey2 + "'";
            while (instancesRetrieved.Count < 1 && DateTime.Now < futureDateTime)
                instancesRetrieved = mapper.Fetch<ClassWithTwoPartitionKeys>(cqlSelect).ToList();
            Assert.AreEqual(1, instancesRetrieved.Count);
            Assert.AreEqual(defaultInstance.PartitionKey1, instancesRetrieved[0].PartitionKey1);
            Assert.AreEqual(defaultInstance.PartitionKey2, instancesRetrieved[0].PartitionKey2);

            var err = Assert.Throws<InvalidQueryException>(() => mapper.Fetch<ClassWithTwoPartitionKeys>("SELECT * from \"" + table.Name + "\" where \"PartitionKey1\" = '" + instance.PartitionKey1 + "'"));
            string expectedErrMsg = "Partition key part(s:)? PartitionKey2 must be restricted (since preceding part is|as other parts are)";
            if (CassandraVersion >= Version.Parse("3.10"))
            {
                expectedErrMsg = "Cannot execute this query as it might involve data filtering and thus may have unpredictable performance. If you want to execute this query despite the performance unpredictability, use ALLOW FILTERING";
            }
            StringAssert.IsMatch(expectedErrMsg, err.Message);

            Assert.Throws<InvalidQueryException>(() => mapper.Fetch<ClassWithTwoPartitionKeys>("SELECT * from \"" + table.Name + "\" where \"PartitionKey2\" = '" + instance.PartitionKey2 + "'"));
        }

        /// <summary>
        /// Successfully insert a new record into a table that was created with fluent mapping, 
        /// using Session.Execute to insert an Insert object created with table.Insert()
        /// </summary>
        [Test, TestCassandraVersion(2,0)]
        public void Insert_WithSessionExecuteTableInsert()
        {
            // Setup
            string uniqueTableName = TestUtils.GetUniqueTableName().ToLowerInvariant();
            var mappingConfig = new MappingConfiguration().Define(new Map<lowercaseclassnamepklowercase>().TableName(uniqueTableName).PartitionKey(c => c.somepartitionkey).CaseSensitive());
            var table = new Table<lowercaseclassnamepklowercase>(_session, mappingConfig);
            table.Create();

            // Insert the data
            lowercaseclassnamepklowercase defaultPocoInstance = new lowercaseclassnamepklowercase();
            _session.Execute(table.Insert(defaultPocoInstance));
            var mapper = new Mapper(_session, mappingConfig);
            List<lowercaseclassnamepklowercase> instancesQueried = mapper.Fetch<lowercaseclassnamepklowercase>("SELECT * from " + table.Name).ToList();
            Assert.AreEqual(1, instancesQueried.Count);
            lowercaseclassnamepklowercase defaultInstance = new lowercaseclassnamepklowercase();
            Assert.AreEqual(defaultInstance.somepartitionkey, instancesQueried[0].somepartitionkey);
        }

        /// <summary>
        /// Attempt to insert a Poco into a nonexistent table
        /// </summary>
        [Test]
        public void Insert_UnconfiguredTable()
        {
            // Setup
            var mapper = new Mapper(_session, new MappingConfiguration());
            ManyDataTypesPoco manyTypesPoco = ManyDataTypesPoco.GetRandomInstance();

            // Validate Error Message
            var e = Assert.Throws<InvalidQueryException>(() => mapper.Insert(manyTypesPoco));
            StringAssert.IsMatch(typeof(ManyDataTypesPoco).Name.ToLower(), e.Message);
        }

        /// <summary>
        /// By default Linq preserves class param casing, but cqlpoco does not, 
        /// so expect "unconfigured columnfamily" when trying to insert via cqlpoco using default settings
        /// This also validates that a private class can be used by the CqlPoco client
        /// </summary>
        [Test]
        public void Insert_ClassAndPartitionKeyAreCamelCase()
        {
            var mappingConfig = new MappingConfiguration().Define(new Map<PrivateClassWithClassNameCamelCase>().PartitionKey(c => c.SomePartitionKey));
            Table<PrivateClassWithClassNameCamelCase> table = new Table<PrivateClassWithClassNameCamelCase>(_session, mappingConfig);
            Assert.AreNotEqual(table.Name, table.Name.ToLower());
            table.Create();

            var mapper = new Mapper(_session, new MappingConfiguration());
            PrivateClassWithClassNameCamelCase privateClassCamelCase = new PrivateClassWithClassNameCamelCase();
            mapper.Insert(privateClassCamelCase);

            List<lowercaseclassnamepklowercase> instancesQueried = mapper.Fetch<lowercaseclassnamepklowercase>("SELECT * from " + table.Name).ToList();
            Assert.AreEqual(1, instancesQueried.Count);
            lowercaseclassnamepklowercase defaultInstance = new lowercaseclassnamepklowercase();
            Assert.AreEqual(defaultInstance.somepartitionkey, instancesQueried[0].somepartitionkey);

            Assert.Throws<InvalidQueryException>(() => TestUtils.TableExists(_session, _uniqueKsName, typeof (PrivateClassWithClassNameCamelCase).Name, true));
            Assert.IsTrue(TestUtils.TableExists(_session, _uniqueKsName, typeof(PrivateClassWithClassNameCamelCase).Name.ToLower(), true));
        }

        /// <summary>
        /// Validate that mapped class properties are lower-cased by default
        /// </summary>
        [Test]
        public void Insert_TableNameLowerCase_PartitionKeyCamelCase()
        {
            // Setup
            var mappingConfig = new MappingConfiguration().Define(new Map<lowercaseclassnamepkcamelcase>().PartitionKey(c => c.SomePartitionKey));
            Table<lowercaseclassnamepkcamelcase> table = new Table<lowercaseclassnamepkcamelcase>(_session, mappingConfig);
            Assert.AreEqual(table.Name, table.Name.ToLower());
            table.Create();
            var mapper = new Mapper(_session, new MappingConfiguration());
            lowercaseclassnamepkcamelcase privateClassInstance = new lowercaseclassnamepkcamelcase();

            // Validate state of table
            mapper.Insert(privateClassInstance);
            List<lowercaseclassnamepkcamelcase> instancesQueried = mapper.Fetch<lowercaseclassnamepkcamelcase>("SELECT * from " + table.Name).ToList();
            Assert.AreEqual(1, instancesQueried.Count);
            lowercaseclassnamepkcamelcase defaultPocoInstance = new lowercaseclassnamepkcamelcase();
            Assert.AreEqual(defaultPocoInstance.SomePartitionKey, instancesQueried[0].SomePartitionKey);

            // Attempt to select from Camel Case partition key
            string cqlCamelCasePartitionKey = "SELECT * from " + typeof (lowercaseclassnamepkcamelcase).Name + " where \"SomePartitionKey\" = 'doesntmatter'";
            var ex = Assert.Throws<InvalidQueryException>(() => _session.Execute(cqlCamelCasePartitionKey));
            var expectedErrMsg = "Undefined name SomePartitionKey in where clause";
            if (CassandraVersion >= Version.Parse("3.10"))
            {
                expectedErrMsg = "Undefined column name \"SomePartitionKey\"";
            }
            StringAssert.Contains(expectedErrMsg, ex.Message);

            // Validate that select on lower case key does not fail
            string cqlLowerCasePartitionKey = "SELECT * from " + typeof(lowercaseclassnamepkcamelcase).Name + " where \"somepartitionkey\" = '" + defaultPocoInstance.SomePartitionKey + "'";
            List<Row> rows = _session.Execute(cqlLowerCasePartitionKey).GetRows().ToList();
            Assert.AreEqual(1, rows.Count);
            Assert.AreEqual(defaultPocoInstance.SomePartitionKey, rows[0].GetValue<string>("somepartitionkey"));
        }

        /// <summary>
        /// Attempting to insert a Poco into a table with a missing column field fails
        /// </summary>
        [Test]
        public void Insert_MislabledClusteringKey()
        {
            string tableName = typeof(PocoWithAdditionalField).Name.ToLower();
            string createTableCql = "Create table " + tableName + "(somestring text PRIMARY KEY)";
            _session.Execute(createTableCql);
            var cqlClient = new Mapper(_session, new MappingConfiguration());
            PocoWithAdditionalField pocoWithCustomAttributes = new PocoWithAdditionalField();

            // Validate expected exception
            var ex = Assert.Throws<InvalidQueryException>(() => cqlClient.Insert(pocoWithCustomAttributes));
            var expectedMessage = "Unknown identifier someotherstring";
            if (CassandraVersion >= Version.Parse("3.10"))
            {
                expectedMessage = "Undefined column name someotherstring";
            }
            StringAssert.Contains(expectedMessage, ex.Message);
        }

        [Test]
        public void InsertIfNotExists_Applied_Test()
        {
            var config = new MappingConfiguration()
                .Define(new Map<Song>().PartitionKey(s => s.Id).TableName("song_insert"));
            //Use linq to create the table
            new Table<Song>(_session, config).Create();
            var mapper = new Mapper(_session, config);
            var song = new Song {Id = Guid.NewGuid(), Artist = "Led Zeppelin", Title = "Good Times Bad Times"};
            //It is the first song there, it should apply it
            var appliedInfo = mapper.InsertIfNotExists(song);
            Assert.True(appliedInfo.Applied);
            Assert.Null(appliedInfo.Existing);
            //Following times, it should not apply the mutation as the partition key is the same
            var nextSong = new Song { Id = song.Id, Title = "Communication Breakdown" };
            appliedInfo = mapper.InsertIfNotExists(nextSong);
            Assert.False(appliedInfo.Applied);
            Assert.NotNull(appliedInfo.Existing);
            Assert.AreEqual(song.Title, appliedInfo.Existing.Title);
        }

        [Test]
        public void Insert_Without_Nulls_Test()
        {
            var config = new MappingConfiguration()
                .Define(new Map<Song>().PartitionKey(s => s.Id).TableName("song_insert"));
            //Use linq to create the table
            new Table<Song>(_session, config).CreateIfNotExists();
            var mapper = new Mapper(_session, config);
            var song = new Song 
            { 
                Id = Guid.NewGuid(), 
                Artist = "The Who", 
                Title = "Substitute", 
                ReleaseDate = DateTimeOffset.UtcNow
            };
            mapper.Insert(song);
            var storedSong = mapper.First<Song>("WHERE id = ?", song.Id);
            Assert.AreEqual(song.Artist, storedSong.Artist);
            //do NOT insert nulls
            mapper.Insert(new Song { Id = song.Id, Artist = null, Title = "Substitute 2", ReleaseDate = DateTimeOffset.UtcNow}, false);
            //it should have the new title but the artist should still be the same (not null)
            storedSong = mapper.First<Song>("WHERE id = ?", song.Id);
            Assert.NotNull(storedSong.Artist);
            Assert.AreEqual(song.Artist, storedSong.Artist);
            Assert.AreEqual("Substitute 2", storedSong.Title);
            //Now insert nulls
            mapper.Insert(new Song { Id = song.Id, Artist = null, Title = "Substitute 3", ReleaseDate = DateTimeOffset.UtcNow }, true);
            //it should have the new title and the artist should be null
            storedSong = mapper.First<Song>("WHERE id = ?", song.Id);
            Assert.Null(storedSong.Artist);
            Assert.AreEqual("Substitute 3", storedSong.Title);
        }

        [Test]
        public void Insert_With_TTL()
        {
            var config = new MappingConfiguration()
                .Define(new Map<Song>().PartitionKey(s => s.Id).TableName("song_insert"));
            //Use linq to create the table
            new Table<Song>(_session, config).CreateIfNotExists();
            var mapper = new Mapper(_session, config);
            var song = new Song
            {
                Id = Guid.NewGuid(),
                Artist = "The Who",
                Title = "Substitute",
                ReleaseDate = DateTimeOffset.UtcNow
            };
            mapper.Insert(song, true, 5);
            var notExpiredSong = mapper.First<Song>("WHERE id = ?", song.Id);
            Assert.NotNull(notExpiredSong);
            Assert.AreEqual(song.Id, notExpiredSong.Id);
            Assert.AreEqual(song.Artist, notExpiredSong.Artist);
            Assert.AreEqual(song.Title, notExpiredSong.Title);
            Thread.Sleep(6000);
            var expiredSong = mapper.FirstOrDefault<Song>("WHERE id = ?", song.Id);
            Assert.Null(expiredSong);
        }


        /// Tests if timestamp is set on CqlQueryOptions.
        ///
        /// The TIMESTAMP input is in microseconds. If not specified, the time (in microseconds) that the write occurred to the column is used. 
        /// when all nodes in the cluster is down, given a set ReadTimeoutMillis of 3 seconds.
        ///
        /// @since 2.1
        /// @jira_ticket CSHARP-409
        /// @expected_result The date/time that the column was written should be the same as set.
        ///
        /// @test_category Insert:Timestamp
        [Test]
        [TestCassandraVersion(2, 1)]
        public void CqlClient_Timestamp()
        {
            var config = new MappingConfiguration()
                .Define(new Map<Song>().PartitionKey(s => s.Id).TableName("song_insert"));
            
            //Use linq to create the table
            var table = new Table<Song>(_session, config);
            table.CreateIfNotExists();
            var mapper = new Mapper(_session, config);
            var song = new Song
            {
                Id = Guid.NewGuid(),
                Artist = "The Who",
                Title = "Substitute",
                ReleaseDate = DateTimeOffset.UtcNow
            };
            //Set timestamp to 1 day ago
            var timestamp = DateTimeOffset.Now.Subtract(TimeSpan.FromDays(1));
            mapper.Insert(song, true, CqlQueryOptions.New().SetTimestamp(timestamp));

            //query for timestamp in a column of the record
            var cqlLowerCasePartitionKey = "SELECT WRITETIME (Artist) AS timestamp FROM " + table.Name + " WHERE Id = " + song.Id + ";";
            var rows = _session.Execute(cqlLowerCasePartitionKey).GetRows().ToList();
            Assert.AreEqual(1, rows.Count);

            var creationTimestamp = rows[0].GetValue<long>("timestamp");
            Assert.NotNull(creationTimestamp);
            //Timestamp retrieved is in macroseconds. Converting it to milliseconds
            Assert.AreEqual(TypeSerializer.SinceUnixEpoch(timestamp).Ticks / 10, rows[0].GetValue<object>("timestamp"));
        }



        [Test]
        public void Insert_Fetch_With_Enum()
        {
            var config = new MappingConfiguration().Define(new Map<PlainUser>()
                .ExplicitColumns()
                .PartitionKey(s => s.UserId)
                .Column(s => s.FavoriteColor, c => c.WithDbType<int>())
                .Column(s => s.Name)
                .Column(s => s.UserId, c => c.WithName("id"))
                .TableName("enum_insert_test"));
            //Use linq to create the table
            new Table<PlainUser>(_session, config).CreateIfNotExists();
            var mapper = new Mapper(_session, config);
            var user = new PlainUser
            {
                UserId = Guid.NewGuid(),
                Name = "My user",
                FavoriteColor = RainbowColor.Orange
            };
            mapper.Insert(user, true, 5);
            var retrievedUser = mapper.First<PlainUser>("WHERE id = ?", user.UserId);
            Assert.NotNull(retrievedUser);
            Assert.AreEqual(user.UserId, retrievedUser.UserId);
            Assert.AreEqual(user.Name, retrievedUser.Name);
            Assert.AreEqual(user.FavoriteColor, retrievedUser.FavoriteColor);
        }

        [Test]
        public void Insert_Batch_With_Options()
        {
            var anotherKeyspace = TestUtils.GetUniqueKeyspaceName().ToLowerInvariant();
            using (var cluster = Cluster.Builder().AddContactPoint(TestCluster.InitialContactPoint)
                                                .WithSocketOptions(new SocketOptions().SetConnectTimeoutMillis(30000))
                                                .Build())
            {
                var session = cluster.Connect();
                session.CreateKeyspace(anotherKeyspace, new Dictionary<string, string>
                {
                    { "class", "SimpleStrategy"},
                    { "replication_factor", "3"}
                });
                session.ChangeKeyspace(anotherKeyspace);

                var config = new MappingConfiguration().Define(new Map<PlainUser>()
                    .ExplicitColumns()
                    .PartitionKey(s => s.UserId)
                    .Column(s => s.FavoriteColor, c => c.WithDbType<int>())
                    .Column(s => s.Name)
                    .Column(s => s.UserId, c => c.WithName("id"))
                    .TableName("batch_with_options_table"));
                //Use linq to create the table
                new Table<PlainUser>(session, config).CreateIfNotExists();
                var mapper = new Mapper(session, config);
                var user1 = new PlainUser
                {
                    UserId = Guid.NewGuid(),
                    Name = "My user",
                    FavoriteColor = RainbowColor.Orange
                };
                var user2 = new PlainUser
                {
                    UserId = Guid.NewGuid(),
                    Name = "My user 2",
                    FavoriteColor = RainbowColor.Blue
                };
                var user3 = new PlainUser
                {
                    UserId = Guid.NewGuid(),
                    Name = "My user 3",
                    FavoriteColor = RainbowColor.Blue
                };
                var batch = mapper.CreateBatch();
                batch.Insert(user1);
                batch.Insert(user2);
                var consistency = ConsistencyLevel.All;
                batch.WithOptions(o => o.SetConsistencyLevel(consistency));
                //Timestamp for BATCH request is supported in Cassandra 2.1 or above.
                if (VersionMatch(new TestCassandraVersion(2, 1), TestClusterManager.CassandraVersion))
                {
                    var timestamp = DateTimeOffset.UtcNow.Subtract(TimeSpan.FromDays(1));
                    batch.WithOptions(o => o.SetTimestamp(timestamp));
                }
                var ex = Assert.Throws<UnavailableException>(() => mapper.Execute(batch));
                Assert.AreEqual(ConsistencyLevel.All, ex.Consistency,
                            "Consistency level of batch exception should be the same as specified at CqlQueryOptions: ALL");
                Assert.AreEqual(3, ex.RequiredReplicas);
                Assert.AreEqual(1, ex.AliveReplicas);
            }
        }

        [Test]
        public void Insert_Poco_With_Enum_Collections_Test()
        {
            Session.Execute(string.Format(PocoWithEnumCollections.DefaultCreateTableCql, "tbl_with_enum_collections"));
            var mapper = new Mapper(Session, new MappingConfiguration().Define(
                PocoWithEnumCollections.DefaultMapping.TableName("tbl_with_enum_collections")));
            var collectionValues = new[]{ HairColor.Blonde, HairColor.Gray, HairColor.Red };
            var mapValues = new SortedDictionary<HairColor, TimeUuid>
            {
                { HairColor.Brown, TimeUuid.NewId() },
                { HairColor.Red, TimeUuid.NewId() }
            };
            var expectedCollection = collectionValues.Select(x => (int) x).ToArray();
            var expectedMap = mapValues.ToDictionary(kv => (int) kv.Key, kv => (Guid) kv.Value);
            var poco = new PocoWithEnumCollections
            {
                Id = 1000L,
                List1 = new List<HairColor>(collectionValues),
                List2 = collectionValues,
                Array1 = collectionValues,
                Set1 = new SortedSet<HairColor>(collectionValues),
                Set2 = new SortedSet<HairColor>(collectionValues),
                Set3 = new HashSet<HairColor>(collectionValues),
                Dictionary1 = new Dictionary<HairColor, TimeUuid>(mapValues),
                Dictionary2 = mapValues,
                Dictionary3 = new SortedDictionary<HairColor, TimeUuid>(mapValues)
            };

            mapper.Insert(poco);

            var statement = new SimpleStatement("SELECT * FROM tbl_with_enum_collections WHERE id = ?", 1000L);
            var row = Session.Execute(statement).First();
            Assert.AreEqual(1000L, row.GetValue<long>("id"));
            Assert.AreEqual(expectedCollection, row.GetValue<IEnumerable<int>>("list1"));
            Assert.AreEqual(expectedCollection, row.GetValue<IEnumerable<int>>("list2"));
            Assert.AreEqual(expectedCollection, row.GetValue<IEnumerable<int>>("array1"));
            Assert.AreEqual(expectedCollection, row.GetValue<IEnumerable<int>>("set1"));
            Assert.AreEqual(expectedCollection, row.GetValue<IEnumerable<int>>("set2"));
            Assert.AreEqual(expectedCollection, row.GetValue<IEnumerable<int>>("set3"));
            Assert.AreEqual(expectedMap, row.GetValue<IDictionary<int, Guid>>("map1"));
            Assert.AreEqual(expectedMap, row.GetValue<IDictionary<int, Guid>>("map2"));
            Assert.AreEqual(expectedMap, row.GetValue<IDictionary<int, Guid>>("map3"));
            
            // BONUS: Attempt insert with null values
            Assert.DoesNotThrow(() => mapper.Insert(new PocoWithEnumCollections { Id = 1001L }));
        }

        /////////////////////////////////////////
        /// Private test classes
        /////////////////////////////////////////

        private class ClassWithTwoPartitionKeys
        {
            public string PartitionKey1 = "somePartitionKey1";
            public string PartitionKey2 = "somePartitionKey2";
        }

        private class PrivateClassWithClassNameCamelCase
        {
            public string SomePartitionKey = "somePartitionKey";
        }

        private class lowercaseclassnamepkcamelcase
        {
            public string SomePartitionKey = "somePartitionKey";
        }

        private class lowercaseclassnamepklowercase
        {
            public string somepartitionkey = "somePartitionKey";
        }

        private class PocoWithAdditionalField
        {
            public string SomeString = "someStringValue";
            public string SomeOtherString = "someOtherStringValue";
        }


    }
}
