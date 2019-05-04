//
//       Copyright (C) 2019 DataStax, Inc.
//
//     Please see the license for details:
//     http://www.datastax.com/terms/datastax-dse-driver-license-terms
//

using System;

using Dse.Graph;
using Dse.Insights;
using Dse.SessionManagement;

namespace Dse.Test.Unit
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