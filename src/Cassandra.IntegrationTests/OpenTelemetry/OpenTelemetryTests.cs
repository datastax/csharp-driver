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
using System.Diagnostics;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Cassandra.IntegrationTests.TestBase;
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

        private readonly List<Activity> _exportedActivities = new List<Activity>();

        public OpenTelemetryTests()
        {
            Sdk.CreateTracerProviderBuilder()
              .AddSource(OpenTelemetrySourceName)
              .AddInMemoryExporter(_exportedActivities)
              .Build();
        }

        [TearDown]
        public void Teardown()
        {
            _exportedActivities.Clear();
        }

        [Category(TestCategory.RealClusterLong)]
        [Test]
        public void AddOpenTelemetry_WithKeyspaceAvailable_DbOperationAndDbNameAreIncluded()
        {
            var keyspace = "system";
            var expectedActivityName = $"{SessionActivityName} {keyspace}";
            var expectedDbNameAttribute = keyspace;
            var cluster = GetNewTemporaryCluster(b => b.AddOpenTelemetryInstrumentation());
            var session = cluster.Connect();

            var statement = new SimpleStatement("SELECT key FROM system.local");
            statement.SetKeyspace(keyspace);
            session.Execute(statement);

           var activity = GetActivities().First(x => x.DisplayName == expectedActivityName);

            ValidateSessionActivityAttributes(activity);

            Assert.AreEqual(expectedActivityName, activity.DisplayName);
            Assert.AreEqual(expectedDbNameAttribute, activity.Tags.First(kvp => kvp.Key == "db.name").Value);
        }

        [Category(TestCategory.RealClusterLong)]
        [Test]
        public void AddOpenTelemetry_WithoutKeyspace_DbNameIsNotIncluded()
        {
            var keyspace = "system";
            var expectedDbNameAttribute = keyspace;
            var cluster = GetNewTemporaryCluster(b => b.AddOpenTelemetryInstrumentation());
            var session = cluster.Connect();

            var statement = new SimpleStatement("SELECT key FROM system.local");
            session.Execute(statement);

            var activity = GetActivities().First(x => x.DisplayName == SessionActivityName);

            ValidateSessionActivityAttributes(activity);

            Assert.AreEqual(SessionActivityName, activity.DisplayName);
            Assert.IsNull(activity.Tags.FirstOrDefault(kvp => kvp.Key == "db.name").Value);
        }

        [Category(TestCategory.RealClusterLong)]
        [Test]
        public void AddOpenTelemetry_WithDefaultOptions_DbStatementIsNotIncludedAsAttribute()
        {
            var cluster = GetNewTemporaryCluster(b => b.AddOpenTelemetryInstrumentation());
            var session = cluster.Connect();

            var statement = new SimpleStatement("SELECT key FROM system.local");
            session.Execute(statement);

            var activity = GetActivities().First(x => x.DisplayName == SessionActivityName);

            ValidateSessionActivityAttributes(activity);

            Assert.IsNull(activity.Tags.FirstOrDefault(kvp => kvp.Key == "db.statement").Value);
        }

        [Category(TestCategory.RealClusterLong)]
        [Test]
        public void AddOpenTelemetry_WithIncludeDatabaseStatementOption_DbStatementIsIncludedAsAttribute()
        {
            var expectedDbStatement = "SELECT key FROM system.local";

            var cluster = GetNewTemporaryCluster(b => b.AddOpenTelemetryInstrumentation(options => options.IncludeDatabaseStatement = true));
            var session = cluster.Connect();

            var statement = new SimpleStatement("SELECT key FROM system.local");
            session.Execute(statement);

            var activity = GetActivities().First(x => x.DisplayName == SessionActivityName);

            ValidateSessionActivityAttributes(activity);

            Assert.AreEqual(expectedDbStatement, activity.Tags.First(kvp => kvp.Key == "db.statement").Value);
        }

        [Category(TestCategory.RealClusterLong)]
        [Test]
        public async Task AddOpenTelemetry_ExecuteAndExecuteAsync_SessionRequestIsParentOfNodeRequest()
        {
            var cluster = GetNewTemporaryCluster(b => b.AddOpenTelemetryInstrumentation());
            var session = cluster.Connect();

            var statement = new SimpleStatement("SELECT key FROM system.local");
            await session.ExecuteAsync(statement).ContinueWith(t =>
            {
                var activities = GetActivities();
                var sessionActivity = activities.First(x => x.DisplayName.StartsWith(SessionActivityName));
                var nodeActivity = activities.First(x => x.DisplayName.StartsWith(NodeActivityName));

                Assert.IsNull(sessionActivity.ParentId);
                Assert.AreEqual(sessionActivity.TraceId, nodeActivity.TraceId);
                Assert.AreEqual(sessionActivity.SpanId, nodeActivity.ParentSpanId);

                ValidateSessionActivityAttributes(sessionActivity);
                ValidateNodeActivityAttributes(nodeActivity);
            }).ConfigureAwait(false);

            _exportedActivities.Clear();

            session.Execute(statement);

            var syncActivities = GetActivities();
            var syncSessionActivity = syncActivities.First(x => x.DisplayName == SessionActivityName);
            var syncNodeActivity = syncActivities.First(x => x.DisplayName == NodeActivityName);

            Assert.IsNull(syncSessionActivity.ParentId);
            Assert.AreEqual(syncSessionActivity.TraceId, syncNodeActivity.TraceId);
            Assert.AreEqual(syncSessionActivity.SpanId, syncNodeActivity.ParentSpanId);

            ValidateSessionActivityAttributes(syncSessionActivity);
            ValidateNodeActivityAttributes(syncNodeActivity);
        }

        [Category(TestCategory.RealClusterLong)]
        [Test]
        public async Task AddOpenTelemetry_MapperAndMapperAsync_SessionRequestIsParentOfNodeRequest()
        {
            var testProfile = "testProfile";
            var keyspace = TestUtils.GetUniqueKeyspaceName().ToLowerInvariant();

            var cluster = GetNewTemporaryCluster(b => b
                .AddOpenTelemetryInstrumentation()
                .WithExecutionProfiles(opts => opts
                                  .WithProfile(testProfile, profile => profile
                                      .WithConsistencyLevel(ConsistencyLevel.One))));

            var session = cluster.Connect();

            session.CreateKeyspaceIfNotExists(keyspace);

            session.ChangeKeyspace(keyspace);

            string createTableQuery = @"
            CREATE TABLE IF NOT EXISTS song (
                Artist text,
                Title text,
                Id uuid,
                ReleaseDate timestamp,
                PRIMARY KEY (id)
            )";

            session.Execute(createTableQuery);

            var mapper = new Mapper(session);

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
                Artist = "Pink Floyd",
                Title = "The Dark Side Of The Moon",
                ReleaseDate = DateTimeOffset.UtcNow
            };

            var mappingConfig = new MappingConfiguration()
                .Define(new Map<Song>().PartitionKey(s => s.Id).TableName("song").KeyspaceName(keyspace));

            // Clear activities to get the async Mapping one
            _exportedActivities.Clear();

            await mapper.InsertIfNotExistsAsync(songOne, testProfile, true, null)
                .ContinueWith(t =>
                {
                    var activities = GetActivities();
                    var sessionActivity = activities.First(x => x.DisplayName.StartsWith(SessionActivityName));
                    var nodeActivity = activities.First(x => x.DisplayName.StartsWith(NodeActivityName));

                    Assert.IsNull(sessionActivity.ParentId);
                    Assert.AreEqual(sessionActivity.TraceId, nodeActivity.TraceId);
                    Assert.AreEqual(sessionActivity.SpanId, nodeActivity.ParentSpanId);

                    ValidateSessionActivityAttributes(sessionActivity);
                    ValidateNodeActivityAttributes(nodeActivity);
                }
                ).ConfigureAwait(false);

            // Clear activities to get the sync Mapping one
            _exportedActivities.Clear();

            mapper.InsertIfNotExists(songOne, testProfile, true, null);

            var syncActivities = GetActivities();
            var syncSessionActivity = syncActivities.First(x => x.DisplayName.StartsWith(SessionActivityName));
            var syncNodeActivity = syncActivities.First(x => x.DisplayName.StartsWith(NodeActivityName));

            Assert.IsNull(syncSessionActivity.ParentId);
            Assert.AreEqual(syncSessionActivity.TraceId, syncNodeActivity.TraceId);
            Assert.AreEqual(syncSessionActivity.SpanId, syncNodeActivity.ParentSpanId);

            ValidateSessionActivityAttributes(syncSessionActivity);
            ValidateNodeActivityAttributes(syncNodeActivity);
        }

        private List<Activity> GetActivities()
        {
            var count = 0;

            while(count < 5)
            {
                Thread.Sleep(2000);

                if (_exportedActivities.FirstOrDefault() != null)
                {
                    return _exportedActivities;
                }

                count++;
            }

            return _exportedActivities;
        }

        private static void ValidateSessionActivityAttributes(Activity activity)
        {
            var expectedActivityKind = ActivityKind.Client;
            var expectedTags = new Dictionary<string, string>()
            {
                {"db.system", "cassandra" },
                {"db.operation", "Session Request" },
            };

            Assert.AreEqual(activity.Kind, expectedActivityKind);
            
            var tags = activity.Tags;
            
            foreach (var pair in expectedTags)
            {
                Assert.AreEqual(tags.FirstOrDefault(x => x.Key == pair.Key).Value, expectedTags[pair.Key]);
            }
        }

        private static void ValidateNodeActivityAttributes(Activity activity)
        {
            var expectedActivityKind = ActivityKind.Client;
            var expectedTags = new Dictionary<string, string>()
            {
                {"db.system", "cassandra" },
                {"db.operation", "Node Request" },
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
    }
}
