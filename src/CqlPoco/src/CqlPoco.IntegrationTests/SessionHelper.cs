using System;
using System.Configuration;
using Cassandra;

namespace CqlPoco.IntegrationTests
{
    /// <summary>
    /// Manages a static ISession instance for use by all integration tests.
    /// </summary>
    public static class SessionHelper
    {
        private const string ContactPointAppSettingsKey = "clusterContactPoint";

        private static string _keyspaceName;

        /// <summary>
        /// A static ISession instance for use by all integration tests.
        /// </summary>
        public static ISession Session { get; set; }

        /// <summary>
        /// Initializes the static Session instance by connecting to the C* Cluster and creating a keyspace for
        /// the integration tests.
        /// </summary>
        public static void InitSessionAndKeyspace()
        {
            // Create the cluster
            string contactPoint = ConfigurationManager.AppSettings[ContactPointAppSettingsKey];
            Cluster cluster = Cluster.Builder().AddContactPoint(contactPoint).Build();

            // Create a unique keyspace for each integration test run
            _keyspaceName = string.Format("CqlPoco_{0}", DateTimeOffset.UtcNow.UtcTicks);
            ISession session = cluster.Connect();
            session.CreateKeyspaceIfNotExists(_keyspaceName);
            session.ChangeKeyspace(_keyspaceName);

            Session = session;
        }

        /// <summary>
        /// Removes the keyspace for this test run.
        /// </summary>
        public static void RemoveKeyspace()
        {
            // Remove the keyspace we created on init
            Session.DeleteKeyspaceIfExists(_keyspaceName);
        }
    }
}
