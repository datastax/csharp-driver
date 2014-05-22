using Cassandra;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace CassandraSamples
{
    /// <summary>
    /// Handles the schema creation that it is going to be used in the samples
    /// </summary>
    public static class SchemaHelper
    {
        /// <summary>
        /// Creates the schema that it is going to be used in the samples.
        /// It is grouped into a helper to let the other classes to be cleaner.
        /// </summary>
        public static void CreateSchema(Cluster cluster)
        {
            //This part is not important for the sample purpose.
            //TL;DR
            var session = cluster.Connect();
            CreateKeyspace(session);
            CreateTimeSeriesTable(session);
        }

        private static void CreateKeyspace(ISession session)
        {
            session.Execute("DROP KEYSPACE IF EXISTS driver_samples_kp");
            session.Execute("CREATE KEYSPACE IF NOT EXISTS driver_samples_kp WITH replication = { 'class': 'SimpleStrategy', 'replication_factor' : 1};");
        }

        private static void CreateTimeSeriesTable(ISession session)
        {
            var createCql = @"
                CREATE TABLE driver_samples_kp.temperature_by_day (
                   weatherstation_id text,
                   date text,
                   event_time timestamp,
                   temperature decimal,
                   PRIMARY KEY ((weatherstation_id,date),event_time)
                )";
            session.Execute(createCql);
        }
    }
}
