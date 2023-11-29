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
            var expectedActivityName = $"ExecuteAsync {keyspace}";
            var expectedDbNameAttribute = keyspace;
            var cluster = GetNewTemporaryCluster(b => b.AddOpenTelemetryInstrumentation());
            var session = cluster.Connect();

            var statement = new SimpleStatement("SELECT key FROM system.local");
            statement.SetKeyspace(keyspace);
            session.ExecuteAsync(statement);

            var activity = GetActivity();

            ValidateCommonAttributes(activity);

            Assert.AreEqual(expectedActivityName, activity.DisplayName);
            Assert.AreEqual(expectedDbNameAttribute, activity.Tags.First(kvp => kvp.Key == "db.name").Value);
        }

        [Category(TestCategory.RealClusterLong)]
        [Test]
        public void AddOpenTelemetry_WithoutKeyspace_DbNameIsNotIncluded()
        {
            var keyspace = "system";
            var expectedActivityName = $"ExecuteAsync";
            var expectedDbNameAttribute = keyspace;
            var cluster = GetNewTemporaryCluster(b => b.AddOpenTelemetryInstrumentation());
            var session = cluster.Connect();

            var statement = new SimpleStatement("SELECT key FROM system.local");
            session.ExecuteAsync(statement);

            var activity = GetActivity();

            ValidateCommonAttributes(activity);

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

            var activity = GetActivity();

            ValidateCommonAttributes(activity);

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

            var activity = GetActivity();

            ValidateCommonAttributes(activity);

            Assert.AreEqual(expectedDbStatement, activity.Tags.First(kvp => kvp.Key == "db.statement").Value);
        }

        private Activity GetActivity()
        {
            var count = 0;

            while(count < 5)
            {
                if(_exportedActivities.FirstOrDefault() != null)
                {
                    return _exportedActivities.FirstOrDefault();
                }

                count++;
                Thread.Sleep(1000);
            }

            return _exportedActivities.FirstOrDefault();
        }
        
        private static void ValidateCommonAttributes(Activity activity)
        {
            var expectedActivityKind = ActivityKind.Client;
            var expectedTags = new Dictionary<string, string>()
            {
                {"db.system", "cassandra" },
                {"db.operation", "ExecuteAsync" },
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
