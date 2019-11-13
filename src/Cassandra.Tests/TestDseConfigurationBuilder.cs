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

using Cassandra.Graph;
using Cassandra.Insights;
using Cassandra.SessionManagement;

namespace Cassandra.Tests
{
    internal class TestDseConfigurationBuilder
    {
        public string ApplicationVersion { get; set; }

        public string ApplicationName { get; set; }

        public Guid ClusterId { get; set; } = Guid.NewGuid();

        public IAddressTranslator AddressTranslator { get; set; }

        public Configuration CassandraConfiguration { get; set; }

        public GraphOptions GraphOptions { get; set; } = new GraphOptions();

        public ISessionFactoryBuilder<IInternalDseCluster, IInternalDseSession> DseSessionFactoryBuilder { get; set; }

        public IDseSessionManagerFactory DseSessionManagerFactory { get; set; }

        public MonitorReportingOptions MonitorReportingOptions { get; set; }

        public IInsightsSupportVerifier InsightsSupportVerifier { get; set; }

        public TestDseConfigurationBuilder(Configuration cassandraConfiguration)
        {
            CassandraConfiguration = cassandraConfiguration;
        }

        public DseConfiguration Build()
        {
            return new DseConfiguration(
                CassandraConfiguration,
                GraphOptions,
                ClusterId,
                ApplicationVersion,
                ApplicationName,
                MonitorReportingOptions,
                AddressTranslator,
                InsightsSupportVerifier,
                DseSessionManagerFactory,
                DseSessionFactoryBuilder);
        }
    }
}