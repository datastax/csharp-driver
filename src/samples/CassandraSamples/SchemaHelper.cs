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

﻿using Cassandra;
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
            CreateForumTables(session);
        }

        private static void CreateKeyspace(ISession session)
        {
            session.Execute("DROP KEYSPACE IF EXISTS driver_samples_kp");
            session.Execute("CREATE KEYSPACE IF NOT EXISTS driver_samples_kp WITH replication = { 'class': 'SimpleStrategy', 'replication_factor' : 1};");
        }

        private static void CreateTimeSeriesTable(ISession session)
        {
            //These are not the codez you are looking for ...
            //Boring code just intended to create the schema
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

        public static void CreateForumTables(ISession session)
        {
            var createTopicCql = @"
                CREATE TABLE driver_samples_kp.topics (
                    topic_id uuid PRIMARY KEY,
                    topic_title text,
                    topic_date timestamp
                )";
            session.Execute(createTopicCql);

            var createMessageCql = @"
                CREATE TABLE driver_samples_kp.messages (
                    topic_id uuid,
                    message_date timestamp,
                    message_body text,
                    PRIMARY KEY (topic_id, message_date)
                )";
            session.Execute(createMessageCql);
        }
    }
}
