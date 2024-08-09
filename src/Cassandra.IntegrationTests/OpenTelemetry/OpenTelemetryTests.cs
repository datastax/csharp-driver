using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Threading;
using Cassandra.OpenTelemetry;
using Cassandra.Tests;
using NUnit.Framework;
using OpenTelemetry;
using OpenTelemetry.Trace;

namespace Cassandra.IntegrationTests.OpenTelemetry
{
    [Category(TestCategory.Short)]
    public class OpenTelemetryTests : SharedClusterTest
    {
        private const string OpenTelemetrySourceName = "Cassandra.OpenTelemetry";

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
            var expectedActivityName = $"Session Request {keyspace}";
            var expectedDbNameAttribute = keyspace;
            var cluster = GetNewTemporaryCluster(b => b.AddOpenTelemetryInstrumentation());
            var session = cluster.Connect();

            var statement = new SimpleStatement("SELECT key FROM system.local");
            statement.SetKeyspace(keyspace);
            session.ExecuteAsync(statement);

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
            var expectedActivityName = $"Session Request";
            var expectedDbNameAttribute = keyspace;
            var cluster = GetNewTemporaryCluster(b => b.AddOpenTelemetryInstrumentation());
            var session = cluster.Connect();

            var statement = new SimpleStatement("SELECT key FROM system.local");
            session.ExecuteAsync(statement);

            var activity = GetActivities().First(x => x.DisplayName == expectedActivityName);

            ValidateSessionActivityAttributes(activity);

            Assert.AreEqual(expectedActivityName, activity.DisplayName);
            Assert.IsNull(activity.Tags.FirstOrDefault(kvp => kvp.Key == "db.name").Value);
        }

        [Category(TestCategory.RealClusterLong)]
        [Test]
        public void AddOpenTelemetry_WithDefaultOptions_DbStatementIsNotIncludedAsAttribute()
        {
            var cluster = GetNewTemporaryCluster(b => b.AddOpenTelemetryInstrumentation());
            var session = cluster.Connect();

            var statement = new SimpleStatement("SELECT key FROM system.local");
            session.ExecuteAsync(statement);

            var activity = GetActivities().First();

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
            session.ExecuteAsync(statement);

            var activity = GetActivities().First();

            ValidateSessionActivityAttributes(activity);

            Assert.AreEqual(expectedDbStatement, activity.Tags.First(kvp => kvp.Key == "db.statement").Value);
        }

        private List<Activity> GetActivities()
        {
            var count = 0;

            while(count < 5)
            {
                Thread.Sleep(1000);

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
                {"db.address", "127.0.0.1" },
                {"db.port", "9042" },
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
