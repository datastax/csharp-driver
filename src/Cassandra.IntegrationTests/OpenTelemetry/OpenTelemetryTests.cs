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
using System.Collections;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Threading.Tasks;
using Cassandra.Data.Linq;
using Cassandra.IntegrationTests.Policies.Util;
using Cassandra.IntegrationTests.SimulacronAPI;
using Cassandra.IntegrationTests.TestBase;
using Cassandra.IntegrationTests.TestClusterManagement.Simulacron;
using Cassandra.Mapping;
using Cassandra.OpenTelemetry;
using Cassandra.Tests;
using Cassandra.Tests.Mapping.Pocos;
using NUnit.Framework;
using OpenTelemetry;
using OpenTelemetry.Trace;

namespace Cassandra.IntegrationTests.OpenTelemetry
{
    [Category(TestCategory.Short)]
    public class OpenTelemetryTests : SharedClusterTest
    {
        private const string OpenTelemetrySourceName = "Cassandra.OpenTelemetry";

        private const string SessionActivityName = "Session Request";

        private const string NodeActivityName = "Node Request";

        private readonly CopyOnReadList<Activity> _exportedActivities = new CopyOnReadList<Activity>();

        private readonly ActivitySource _internalActivitySource = new ActivitySource("testeActivitySource");
        
        private DateTime _testStartDateTime;

        private TracerProvider _sdk;

        [SetUp]
        public void SetUp()
        {
            _sdk = Sdk.CreateTracerProviderBuilder()
                     .AddSource(OpenTelemetrySourceName)
                     .AddSource(_internalActivitySource.Name)
                     .AddInMemoryExporter(_exportedActivities)
                     .Build();
            _testStartDateTime = DateTime.UtcNow;
        }

        [TearDown]
        public void Teardown()
        {
            _sdk.Dispose();
            _exportedActivities.Clear();
        }

        [Category(TestCategory.RealCluster)]
        [Test]
        public void AddOpenTelemetry_WithKeyspaceAvailable_DbOperationAndDbNameAreIncluded()
        {
            var keyspace = "system";
            var expectedActivityName = $"{SessionActivityName} {keyspace}";
            var expectedDbNameAttribute = keyspace;
            var cluster = GetNewTemporaryCluster(b => b.WithOpenTelemetryInstrumentation());
            var session = cluster.Connect();

            var statement = new SimpleStatement("SELECT key FROM system.local");
            statement.SetKeyspace(keyspace);
            session.Execute(statement);

            RetryUntilActivities(_testStartDateTime, expectedActivityName, 1);

           var activity = GetActivities(_testStartDateTime).First(x => x.DisplayName == expectedActivityName);

            ValidateSessionActivityAttributes(activity, typeof(SimpleStatement));

            Assert.AreEqual(expectedActivityName, activity.DisplayName);
            Assert.AreEqual(expectedDbNameAttribute, activity.Tags.First(kvp => kvp.Key == "db.namespace").Value);
        }

        [Category(TestCategory.RealCluster)]
        [Test]
        public void AddOpenTelemetry_WithoutKeyspace_DbNameIsNotIncluded()
        {
            var cluster = GetNewTemporaryCluster(b => b.WithOpenTelemetryInstrumentation());
            var session = cluster.Connect();

            var statement = new SimpleStatement("SELECT key FROM system.local");
            session.Execute(statement);

            RetryUntilActivities(_testStartDateTime, SessionActivityName, 1);

            var activity = GetActivities(_testStartDateTime).First(x => x.DisplayName == SessionActivityName);

            ValidateSessionActivityAttributes(activity, typeof(SimpleStatement));

            Assert.AreEqual(SessionActivityName, activity.DisplayName);
            Assert.IsNull(activity.Tags.FirstOrDefault(kvp => kvp.Key == "db.namespace").Value);
        }

        [Category(TestCategory.RealCluster)]
        [Test]
        public void AddOpenTelemetry_WithDefaultOptions_DbStatementIsNotIncludedAsAttribute()
        {
            var cluster = GetNewTemporaryCluster(b => b.WithOpenTelemetryInstrumentation());
            var session = cluster.Connect();

            var statement = new SimpleStatement("SELECT key FROM system.local");
            session.Execute(statement);

            RetryUntilActivities(_testStartDateTime, SessionActivityName, 1);

            var activity = GetActivities(_testStartDateTime).First(x => x.DisplayName == SessionActivityName);

            ValidateSessionActivityAttributes(activity, typeof(SimpleStatement));

            Assert.IsNull(activity.Tags.FirstOrDefault(kvp => kvp.Key == "db.query.text").Value);
        }

        [Category(TestCategory.RealCluster)]
        [Test]
        public void AddOpenTelemetry_WithIncludeDatabaseStatementOption_DbStatementIsIncludedAsAttribute()
        {
            var expectedDbStatement = "SELECT key FROM system.local";

            var cluster = GetNewTemporaryCluster(b => b.WithOpenTelemetryInstrumentation(options => options.IncludeDatabaseStatement = true));
            var session = cluster.Connect();

            var statement = new SimpleStatement("SELECT key FROM system.local");
            session.Execute(statement);

            RetryUntilActivities(_testStartDateTime, SessionActivityName, 1);

            var activity = GetActivities(_testStartDateTime).First(x => x.DisplayName == SessionActivityName);

            ValidateSessionActivityAttributes(activity, typeof(SimpleStatement));

            Assert.AreEqual(expectedDbStatement, activity.Tags.First(kvp => kvp.Key == "db.query.text").Value);
        }

        [Category(TestCategory.RealCluster)]
        [Test]
        public async Task AddOpenTelemetry_ExecuteAndExecuteAsync_SessionRequestIsParentOfNodeRequest()
        {
            var localDateTime = DateTime.UtcNow;
            var cluster = GetNewTemporaryCluster(b => b.WithOpenTelemetryInstrumentation(opt => opt.IncludeDatabaseStatement = true));
            var session = cluster.Connect();

            var statement = new SimpleStatement("SELECT key FROM system.local");
            await session.ExecuteAsync(statement).ContinueWith(t =>
            {
                RetryUntilActivities(localDateTime, SessionActivityName, 1);
                RetryUntilActivities(localDateTime, NodeActivityName, 1);
                var activities = GetActivities(localDateTime);
                var sessionActivity = activities.First(x => x.DisplayName.StartsWith(SessionActivityName));
                var nodeActivity = activities.First(x => x.DisplayName.StartsWith(NodeActivityName));

                Assert.IsNull(sessionActivity.ParentId);
                Assert.AreEqual(sessionActivity.TraceId, nodeActivity.TraceId);
                Assert.AreEqual(sessionActivity.SpanId, nodeActivity.ParentSpanId);

                ValidateSessionActivityAttributes(sessionActivity, typeof(SimpleStatement));
                ValidateNodeActivityAttributes(nodeActivity, typeof(SimpleStatement));
            }).ConfigureAwait(false);

            localDateTime = DateTime.UtcNow;
            await Task.Delay(200).ConfigureAwait(false);

            session.Execute(statement);

            RetryUntilActivities(localDateTime, SessionActivityName, 1);
            RetryUntilActivities(localDateTime, NodeActivityName, 1);

            var syncActivities = GetActivities(localDateTime);
            var syncSessionActivity = syncActivities.First(x => x.DisplayName == SessionActivityName);
            var syncNodeActivity = syncActivities.First(x => x.DisplayName == NodeActivityName);

            Assert.IsNull(syncSessionActivity.ParentId);
            Assert.AreEqual(syncSessionActivity.TraceId, syncNodeActivity.TraceId);
            Assert.AreEqual(syncSessionActivity.SpanId, syncNodeActivity.ParentSpanId);

            ValidateSessionActivityAttributes(syncSessionActivity, typeof(SimpleStatement));
            ValidateNodeActivityAttributes(syncNodeActivity, typeof(SimpleStatement));
            Assert.Contains(new KeyValuePair<string, string>("db.query.text", "SELECT key FROM system.local"), syncSessionActivity.Tags.ToArray());
            Assert.Contains(new KeyValuePair<string, string>("db.query.text", "SELECT key FROM system.local"), syncNodeActivity.Tags.ToArray());
        }

        [Category(TestCategory.RealCluster)]
        [Test]
        public async Task AddOpenTelemetry_MapperAndMapperAsync_SessionRequestIsParentOfNodeRequest()
        {
            var testProfile = "testProfile";
            var keyspace = TestUtils.GetUniqueKeyspaceName().ToLowerInvariant();

            var cluster = GetNewTemporaryCluster(b => b
                .WithOpenTelemetryInstrumentation(opt => opt.IncludeDatabaseStatement = true)
                .WithExecutionProfiles(opts => opts
                                  .WithProfile(testProfile, profile => profile
                                      .WithConsistencyLevel(ConsistencyLevel.One))));

            var session = cluster.Connect();

            session.CreateKeyspaceIfNotExists(keyspace);

            session.ChangeKeyspace(keyspace);

            CreateSongTable(session);

            Task.Delay(100).GetAwaiter().GetResult();

            var localDateTime = DateTime.UtcNow;

            var mapper = new Mapper(session, new MappingConfiguration()
                .Define(new Map<Song>().PartitionKey(s => s.Id).TableName("song").KeyspaceName(keyspace)));

            var songOne = new Song
            {
                Id = Guid.NewGuid(),
                Artist = "Led Zeppelin",
                Title = "Mothership",
                ReleaseDate = DateTimeOffset.UtcNow
            };

            await mapper.InsertIfNotExistsAsync(songOne, testProfile, true, null)
                .ContinueWith(t =>
                {
                    RetryUntilActivities(localDateTime, SessionActivityName, 1, displayNameStartsWith: true);
                    RetryUntilActivities(localDateTime, NodeActivityName, 1, displayNameStartsWith: true);
                    var activities = GetActivities(localDateTime);
                    var sessionActivity = activities.First(x => x.DisplayName.StartsWith(SessionActivityName));
                    var nodeActivity = activities.First(x => x.DisplayName.StartsWith(NodeActivityName));

                    Assert.IsNull(sessionActivity.ParentId);
                    Assert.AreEqual(sessionActivity.TraceId, nodeActivity.TraceId);
                    Assert.AreEqual(sessionActivity.SpanId, nodeActivity.ParentSpanId);

                    ValidateSessionActivityAttributes(sessionActivity, typeof(BoundStatement));
                    ValidateNodeActivityAttributes(nodeActivity, typeof(BoundStatement));
                }
                ).ConfigureAwait(false);

            // Filter activity time to get the sync Mapping one
            await Task.Delay(200).ConfigureAwait(false);
            localDateTime = DateTime.UtcNow;

            mapper.InsertIfNotExists(songOne, testProfile, true, null);

            RetryUntilActivities(localDateTime, $"{SessionActivityName} {keyspace}", 1);
            RetryUntilActivities(localDateTime, $"{NodeActivityName} {keyspace}", 1);

            var syncActivities = GetActivities(localDateTime);
            var syncSessionActivity = syncActivities.First(x => x.DisplayName.StartsWith(SessionActivityName));
            var syncNodeActivity = syncActivities.First(x => x.DisplayName.StartsWith(NodeActivityName));

            Assert.IsNull(syncSessionActivity.ParentId);
            Assert.AreEqual(syncSessionActivity.TraceId, syncNodeActivity.TraceId);
            Assert.AreEqual(syncSessionActivity.SpanId, syncNodeActivity.ParentSpanId);

            ValidateSessionActivityAttributes(syncSessionActivity, typeof(BoundStatement));
            ValidateNodeActivityAttributes(syncNodeActivity, typeof(BoundStatement));
            Assert.Contains(new KeyValuePair<string, string>(
                "db.query.text", $"INSERT INTO {keyspace}.song (Artist, Id, ReleaseDate, Title) VALUES (?, ?, ?, ?) IF NOT EXISTS"), syncSessionActivity.Tags.ToArray());
            Assert.Contains(new KeyValuePair<string, string>(
                "db.query.text", $"INSERT INTO {keyspace}.song (Artist, Id, ReleaseDate, Title) VALUES (?, ?, ?, ?) IF NOT EXISTS"), syncNodeActivity.Tags.ToArray());
        }

        [Category(TestCategory.RealCluster)]
        [Test]
        public void AddOpenTelemetry_Linq_SessionRequestIsParentOfNodeRequest()
        {
            var keyspace = TestUtils.GetUniqueKeyspaceName().ToLowerInvariant();

            var cluster = GetNewTemporaryCluster(b => b
                .WithOpenTelemetryInstrumentation());

            var session = cluster.Connect();

            session.CreateKeyspaceIfNotExists(keyspace);

            session.ChangeKeyspace(keyspace);

            CreateSongTable(session);

            Task.Delay(100).GetAwaiter().GetResult();

            _testStartDateTime = DateTime.UtcNow;

            var table = new Table<Song>(session, new MappingConfiguration().Define(new Map<Song>().TableName("song")));

            var song = new Song
            {
                Id = Guid.NewGuid(),
                Artist = "Led Zeppelin",
                Title = "Mothership",
                ReleaseDate = DateTimeOffset.UtcNow
            };

            table.Insert(song).Execute();

            RetryUntilActivities(_testStartDateTime, SessionActivityName, 1, displayNameStartsWith: true);
            RetryUntilActivities(_testStartDateTime, NodeActivityName,  1, displayNameStartsWith: true);
            var syncActivities = GetActivities(_testStartDateTime);

            var syncSessionActivity = syncActivities.First(x => x.DisplayName.StartsWith(SessionActivityName));
            var syncNodeActivity = syncActivities.First(x => x.DisplayName.StartsWith(NodeActivityName));

            Assert.IsNull(syncSessionActivity.ParentId);
            Assert.AreEqual(syncSessionActivity.TraceId, syncNodeActivity.TraceId);
            Assert.AreEqual(syncSessionActivity.SpanId, syncNodeActivity.ParentSpanId);

            ValidateSessionActivityAttributes(syncSessionActivity, typeof(BoundStatement));
            ValidateNodeActivityAttributes(syncNodeActivity, typeof(BoundStatement));
        }

        [Test]
        [Category(TestCategory.RealCluster)]
        public void AddOpenTelemetry_WhenMethodIsInvokedAfterQuery_TraceIdIsTheSameThroughRequest_Sync()
        {
            var firstMethodName = "FirstMethod";
            var secondMethodName = "SecondMethod";

            using (var _ = _internalActivitySource.StartActivity(firstMethodName, ActivityKind.Internal))
            {
                var cluster = GetNewTemporaryCluster(b => b.WithOpenTelemetryInstrumentation());
                var session = cluster.Connect();

                var keyspace = TestUtils.GetUniqueKeyspaceName().ToLowerInvariant();
                session.CreateKeyspaceIfNotExists(keyspace);
                session.ChangeKeyspace(keyspace);

                SimpleStatementMethod(session);
                LinqMethod(session);
                MapperMethod(session);


                SecondMethod(secondMethodName);
            }

            RetryUntilActivities(_testStartDateTime, SessionActivityName, 5);
            RetryUntilActivities(_testStartDateTime, secondMethodName, 1);
            RetryUntilActivities(_testStartDateTime, firstMethodName, 1);
            var activities = GetActivities(_testStartDateTime).ToList();

            var firstMethodActivity = activities.First(x => x.DisplayName == firstMethodName);
            var secondMethodActivity = activities.First(x => x.DisplayName == secondMethodName);
            var sessionActivities = activities.Where(x => x.DisplayName == SessionActivityName).ToList();

            Assert.AreEqual(5, sessionActivities.Count); // 2 x CREATE TABLE IF NOT EXISTS + 1 SELECT + 2 INSERTS

            sessionActivities.ForEach(act =>
            {
                Assert.AreEqual(firstMethodActivity.TraceId, act.TraceId);
                Assert.AreEqual(firstMethodActivity.SpanId, act.ParentSpanId);
            });

            Assert.AreEqual(firstMethodActivity.TraceId, secondMethodActivity.TraceId);
            Assert.AreEqual(firstMethodActivity.SpanId, secondMethodActivity.ParentSpanId);
        }

        [Test]
        [Category(TestCategory.RealCluster)]
        public async Task AddOpenTelemetry_WhenMethodIsInvokedAfterQuery_TraceIdIsTheSameThroughRequest_Async()
        {
            var firstMethodName = "FirstMethod";
            var secondMethodName = "SecondMethod";

            using (var _ = _internalActivitySource.StartActivity(firstMethodName, ActivityKind.Internal))
            {
                var cluster = GetNewTemporaryCluster(b => b.WithOpenTelemetryInstrumentation());
                var session = await cluster.ConnectAsync().ConfigureAwait(false);

                var keyspace = TestUtils.GetUniqueKeyspaceName().ToLowerInvariant();
                session.CreateKeyspaceIfNotExists(keyspace);
                session.ChangeKeyspace(keyspace);

                await SimpleStatementMethodAsync(session).ConfigureAwait(false);
                await LinqMethodAsync(session).ConfigureAwait(false);
                await MapperMethodAsync(session).ConfigureAwait(false);


                SecondMethod(secondMethodName);
            }

            RetryUntilActivities(_testStartDateTime, SessionActivityName, 5);
            RetryUntilActivities(_testStartDateTime, secondMethodName, 1);
            RetryUntilActivities(_testStartDateTime, firstMethodName, 1);
            var activities = GetActivities(_testStartDateTime).ToList();

            var firstMethodActivity = activities.First(x => x.DisplayName == firstMethodName);
            var secondMethodActivity = activities.First(x => x.DisplayName == secondMethodName);
            var sessionActivities = activities.Where(x => x.DisplayName == SessionActivityName).ToList();

            Assert.AreEqual(5, sessionActivities.Count); // 2 x CREATE TABLE IF NOT EXISTS + 1 SELECT + 2 INSERTS

            sessionActivities.ForEach(act =>
            {
                Assert.AreEqual(firstMethodActivity.TraceId, act.TraceId);
                Assert.AreEqual(firstMethodActivity.SpanId, act.ParentSpanId);
            });

            Assert.AreEqual(firstMethodActivity.TraceId, secondMethodActivity.TraceId);
            Assert.AreEqual(firstMethodActivity.SpanId, secondMethodActivity.ParentSpanId);
        }

        [Test]
        public void AddOpenTelemetry_RetryOnNextHost_ShouldProduceOneErrorAndOneValidSpansForTheSameSessionSpan()
        {
            var expectedErrorDescription = "overloaded";

            using (var simulacronCluster = SimulacronCluster.CreateNew(3))
            {
                var contactPoint = simulacronCluster.InitialContactPoint;
                var nodes = simulacronCluster.GetNodes().ToArray();
                var loadBalancingPolicy = new CustomLoadBalancingPolicy(
                    nodes.Select(n => n.ContactPoint).ToArray());

                var builder = ClusterBuilder()
                                     .AddContactPoint(contactPoint)
                                     .WithSocketOptions(new SocketOptions()
                                                        .SetConnectTimeoutMillis(10000)
                                                        .SetReadTimeoutMillis(5000))
                                     .WithLoadBalancingPolicy(loadBalancingPolicy)
                                     .WithRetryPolicy(TryNextHostRetryPolicy.Instance)
                                     .WithOpenTelemetryInstrumentation();

                using (var cluster = builder.Build())
                {
                    var session = (Session)cluster.Connect();
                    const string cql = "select * from table2";

                    nodes[0].PrimeFluent(
                        b => b.WhenQuery(cql).
                               ThenOverloaded(expectedErrorDescription));

                    nodes[1].PrimeFluent(
                        b => b.WhenQuery(cql).
                               ThenRowsSuccess(new[] { ("text", DataType.Ascii) }, rows => rows.WithRow("test1").WithRow("test2")));

                    session.Execute(new SimpleStatement(cql).SetConsistencyLevel(ConsistencyLevel.One));

                    RetryUntilActivities(_testStartDateTime, SessionActivityName, 1);
                    RetryUntilActivities(_testStartDateTime, NodeActivityName, 2);
                    var activities = GetActivities(_testStartDateTime);
                    var sessionActivity = activities.First(x => x.DisplayName.StartsWith(SessionActivityName));
                    var validNodeActivity = activities.First(x => x.DisplayName.StartsWith(NodeActivityName) && x.Status != ActivityStatusCode.Error);
                    var errorNodeActivity = activities.First(x => x.DisplayName.StartsWith(NodeActivityName) && x.Status == ActivityStatusCode.Error);

                    Assert.AreEqual(sessionActivity.TraceId, validNodeActivity.TraceId);
                    Assert.AreEqual(sessionActivity.TraceId, errorNodeActivity.TraceId);
                    Assert.AreEqual(sessionActivity.SpanId, validNodeActivity.ParentSpanId);
                    Assert.AreEqual(sessionActivity.SpanId, errorNodeActivity.ParentSpanId);
                    Assert.AreEqual(expectedErrorDescription, errorNodeActivity.StatusDescription);
                    Assert.True(validNodeActivity.StartTimeUtc > errorNodeActivity.StartTimeUtc);
                }
            }
        }

        [Test]
        [Category(TestCategory.RealCluster)]
        public void AddOpenTelemetry_WithPaginationOnQuery_ShouldMultipleSpansForTheSameTraceId()
        {
            using (var activity = _internalActivitySource.StartActivity("Paging", ActivityKind.Internal))
            {
                var session = GetNewTemporaryCluster(b => b.WithOpenTelemetryInstrumentation()).Connect();

                session.CreateKeyspaceIfNotExists(KeyspaceName, null, false);

                session.ChangeKeyspace(KeyspaceName);

                CreateSongTable(session);

                for (int i = 0; i < 100; i++)
                {
                    session.Execute(
                        $"INSERT INTO {KeyspaceName}.song (Artist, Title, Id, ReleaseDate) VALUES('Pink Floyd', 'The Dark Side Of The Moon', {Guid.NewGuid()}, {((DateTimeOffset)DateTime.UtcNow).ToUnixTimeSeconds()})");
                }

                Task.Delay(100).GetAwaiter().GetResult();

                var localDateTime = DateTime.UtcNow;
                Task.Delay(200).GetAwaiter().GetResult();

                var rs = session.Execute(new SimpleStatement($"SELECT * FROM {KeyspaceName}.song").SetPageSize(1));
                _ = rs.ToList();

                RetryUntilActivities(_testStartDateTime, SessionActivityName, 2, true);
                var sessionActivities = GetActivities(localDateTime).Where(x => x.DisplayName == SessionActivityName).ToList();

                Assert.Greater(sessionActivities.Count, 1);

                var firstActivity = sessionActivities.First();
                var lastActivity = sessionActivities.Last();

                Assert.AreEqual(firstActivity.TraceId, lastActivity.TraceId);
                Assert.AreNotEqual(firstActivity.SpanId, lastActivity.SpanId);
            }
        }

        [Category(TestCategory.RealCluster)]
        [Test]
        public async Task AddOpenTelemetry_Batch_ExpectedStatement()
        {
            var keyspace = TestUtils.GetUniqueKeyspaceName().ToLowerInvariant();

            var cluster = GetNewTemporaryCluster(b => b
                .WithOpenTelemetryInstrumentation(opt => opt.IncludeDatabaseStatement = true));

            var session = cluster.Connect();

            session.CreateKeyspaceIfNotExists(keyspace);

            session.ChangeKeyspace(keyspace);

            CreateSongTable(session);

            Task.Delay(100).GetAwaiter().GetResult();

            var localDateTime = DateTime.UtcNow;

            var mapper = new Mapper(session, new MappingConfiguration()
                .Define(new Map<Song>().PartitionKey(s => s.Id).TableName("song").KeyspaceName(keyspace)));

            var songOne = new Song
            {
                Id = Guid.NewGuid(),
                Artist = "Led Zeppelin",
                Title = "Mothership",
                ReleaseDate = DateTimeOffset.UtcNow
            };

            var songTwo = new Song
            {
                Id = Guid.NewGuid(),
                Artist = "Led Zeppelin",
                Title = "Stairway To Heaven",
                ReleaseDate = DateTimeOffset.UtcNow
            };

            var batch = mapper.CreateBatch(BatchType.Logged);
            batch.Insert(songOne);
            batch.Insert(songTwo);
            await mapper.ExecuteAsync(batch).ConfigureAwait(false);

            RetryUntilActivities(localDateTime, $"{SessionActivityName} {keyspace}", 1);
            RetryUntilActivities(localDateTime, $"{NodeActivityName} {keyspace}", 1);

            var syncActivities = GetActivities(localDateTime);
            var syncSessionActivity = syncActivities.First(x => x.DisplayName.StartsWith(SessionActivityName));
            var syncNodeActivity = syncActivities.First(x => x.DisplayName.StartsWith(NodeActivityName));

            Assert.IsNull(syncSessionActivity.ParentId);
            Assert.AreEqual(syncSessionActivity.TraceId, syncNodeActivity.TraceId);
            Assert.AreEqual(syncSessionActivity.SpanId, syncNodeActivity.ParentSpanId);

            ValidateSessionActivityAttributes(syncSessionActivity, typeof(BatchStatement));
            ValidateNodeActivityAttributes(syncNodeActivity, typeof(BatchStatement));
            var expectedStatement =
                $"INSERT INTO {keyspace}.song (Artist, Id, ReleaseDate, Title) VALUES (?, ?, ?, ?); " +
                $"INSERT INTO {keyspace}.song (Artist, Id, ReleaseDate, Title) VALUES (?, ?, ?, ?)";
            Assert.Contains(new KeyValuePair<string, string>("db.query.text", expectedStatement), syncSessionActivity.Tags.ToArray());
            Assert.Contains(new KeyValuePair<string, string>("db.query.text", expectedStatement), syncNodeActivity.Tags.ToArray());
        }

        private async Task SimpleStatementMethodAsync(ISession session)
        {
            var statement = new SimpleStatement("SELECT key FROM system.local");
            await session.ExecuteAsync(statement).ConfigureAwait(false);
        }

        private void SimpleStatementMethod(ISession session)
        {
            var statement = new SimpleStatement("SELECT key FROM system.local");
            session.Execute(statement);
        }

        private async Task LinqMethodAsync(ISession session)
        {
            CreateSongTable(session);

            var table = new Table<Song>(session, new MappingConfiguration().Define(new Map<Song>().TableName("song")));

            var song = new Song
            {
                Id = Guid.NewGuid(),
                Artist = "Led Zeppelin",
                Title = "Mothership",
                ReleaseDate = DateTimeOffset.UtcNow
            };

            await table.Insert(song).ExecuteAsync().ConfigureAwait(false);
        }

        private void LinqMethod(ISession session)
        {
            CreateSongTable(session);

            var table = new Table<Song>(session, new MappingConfiguration().Define(new Map<Song>().TableName("song")));

            var song = new Song
            {
                Id = Guid.NewGuid(),
                Artist = "Led Zeppelin",
                Title = "Mothership",
                ReleaseDate = DateTimeOffset.UtcNow
            };

            table.Insert(song).Execute();
        }

        private async Task MapperMethodAsync(ISession session)
        {
            CreateSongTable(session);

            var table = new Mapper(session, new MappingConfiguration().Define(new Map<Song>().TableName("song")));

            var song = new Song
            {
                Id = Guid.NewGuid(),
                Artist = "Led Zeppelin",
                Title = "Mothership",
                ReleaseDate = DateTimeOffset.UtcNow
            };

            await table.InsertAsync(song).ConfigureAwait(false);
        }

        private void MapperMethod(ISession session)
        {
            CreateSongTable(session);

            var table = new Mapper(session, new MappingConfiguration().Define(new Map<Song>().TableName("song")));

            var song = new Song
            {
                Id = Guid.NewGuid(),
                Artist = "Led Zeppelin",
                Title = "Mothership",
                ReleaseDate = DateTimeOffset.UtcNow
            };

            table.Insert(song);
        }

        private static void CreateSongTable(ISession session)
        {
            string createTableQuery = @"
            CREATE TABLE IF NOT EXISTS song (
                Artist text,
                Title text,
                Id uuid,
                ReleaseDate timestamp,
                PRIMARY KEY (id)
            )";

            session.Execute(createTableQuery);
        }

        private IEnumerable<Activity> GetActivities(DateTime from)
        {
            return _exportedActivities.Where(x => x.StartTimeUtc >= from);
        }

        private static void ValidateSessionActivityAttributes(Activity activity, Type statementType)
        {
            var expectedActivityKind = ActivityKind.Client;
            var expectedTags = new Dictionary<string, string>()
            {
                {"db.system", "cassandra" },
                {"db.operation.name", $"Session Request - {statementType.Name}" },
            };

            Assert.AreEqual(activity.Kind, expectedActivityKind);
            
            var tags = activity.Tags;
            
            foreach (var pair in expectedTags)
            {
                Assert.AreEqual(tags.FirstOrDefault(x => x.Key == pair.Key).Value, expectedTags[pair.Key]);
            }
        }

        private static void ValidateNodeActivityAttributes(Activity activity, Type statementType)
        {
            var expectedActivityKind = ActivityKind.Client;
            var expectedTags = new Dictionary<string, string>()
            {
                {"db.system", "cassandra" },
                {"db.operation.name", $"Node Request - {statementType.Name}" },
                {"server.address", "127.0.0.1" },
                {"server.port", "9042" },
            };

            Assert.AreEqual(activity.Kind, expectedActivityKind);

            var tags = activity.Tags;

            foreach (var pair in expectedTags)
            {
                Assert.AreEqual(tags.FirstOrDefault(x => x.Key == pair.Key).Value, expectedTags[pair.Key]);
            }
        }

        private void SecondMethod(string activityName)
        {
            using (var activity = _internalActivitySource.StartActivity(activityName, ActivityKind.Internal))
            {
                activity.AddTag("db.test", "t");
            }
        }

        private void RetryUntilActivities(DateTime dt, string displayName, int count, bool greaterOrEqual = false, bool displayNameStartsWith = false, int retries = 50, int delayPerRetryMs = 100)
        {
            var lastCount = 0;
            for (var i = 0; i < retries; i++)
            {
                var activities = GetActivities(dt);
                lastCount = displayNameStartsWith ? activities.Count(a => a.DisplayName.StartsWith(displayName)) : activities.Count(a => a.DisplayName == displayName);
                if (greaterOrEqual)
                {
                    if (lastCount >= count)
                    {
                        return;
                    }
                }
                else
                {
                    if (lastCount == count)
                    {
                        return;
                    }
                }
                Task.Delay(delayPerRetryMs).GetAwaiter().GetResult();
            }
            Assert.Fail($"Could not find the expected number of activities (expected {(greaterOrEqual ? ">=" : "==")} {count}, found {lastCount}) with name {displayName}");
        }

        private class CopyOnReadList<T> : ICollection<T>
        {
            private readonly object _writeLock = new object();
            private readonly ICollection<T> _collectionImplementation = new List<T>();

            public int Count
            {
                get
                {
                    lock (_writeLock)
                    {
                        return _collectionImplementation.Count;
                    }
                }
            }

            public bool IsReadOnly => false;

            public IEnumerator<T> GetEnumerator()
            {
                return NewSnapshot().GetEnumerator();
            }

            IEnumerator IEnumerable.GetEnumerator()
            {
                return ((IEnumerable)NewSnapshot()).GetEnumerator();
            }

            public void Add(T item)
            {
                lock (_writeLock)
                {
                    _collectionImplementation.Add(item);
                }
            }

            public void Clear()
            {
                lock (_writeLock)
                {
                    _collectionImplementation.Clear();
                }
            }

            public bool Contains(T item)
            {
                lock (_writeLock)
                {
                    return _collectionImplementation.Contains(item);
                }
            }

            public void CopyTo(T[] array, int arrayIndex)
            {
                lock (_writeLock)
                {
                    _collectionImplementation.CopyTo(array, arrayIndex);
                }
            }

            public bool Remove(T item)
            {
                lock (_writeLock)
                {
                    return _collectionImplementation.Remove(item);
                }
            }

            private IEnumerable<T> NewSnapshot()
            {
                lock (_writeLock)
                {
                    return _collectionImplementation.ToList();
                }
            }
        }
    }
}
