using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Cassandra;
using Dse.Graph;

namespace Dse
{
    /// <summary>
    /// Represents the configuration of a <see cref="DseCluster"/>.
    /// </summary>
    public class DseConfiguration
    {
        /// <summary>
        /// Gets the configuration related to DSE Cassandra Daemon.
        /// </summary>
        public Configuration CassandraConfiguration { get; protected set; }

        /// <summary>
        /// Gets the options related to graph instance.
        /// </summary>
        public GraphOptions GraphOptions { get; protected set; }

        public DseConfiguration(Configuration cassandraConfiguration, GraphOptions graphOptions)
        {
            CassandraConfiguration = cassandraConfiguration;
            GraphOptions = graphOptions;
        }
    }

}
