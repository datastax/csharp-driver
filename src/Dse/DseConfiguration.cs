//
//  Copyright (C) 2016 DataStax, Inc.
//
//  Please see the license for details:
//  http://www.datastax.com/terms/datastax-dse-driver-license-terms
//
using System;
using System.Collections.Generic;
using System.Linq;
using System.Net;
using System.Text;
using Dse;
using Dse.Graph;

namespace Dse
{
    /// <summary>
    /// Represents the configuration of a <see cref="DseCluster"/>.
    /// </summary>
    public class DseConfiguration
    {
        /// <summary>
        /// To be replaced with CassandraConfiguration.AddressTranslator after CSHARP-444.
        /// </summary>
        internal IAddressTranslator AddressTranslator { get; set; }
        /// <summary>
        /// Gets the configuration related to DSE Cassandra Daemon.
        /// </summary>
        public Configuration CassandraConfiguration { get; protected set; }

        /// <summary>
        /// Gets the options related to graph instance.
        /// </summary>
        public GraphOptions GraphOptions { get; protected set; }

        /// <summary>
        /// Creates a new instance of <see cref="DseConfiguration"/>.
        /// </summary>
        public DseConfiguration(Configuration cassandraConfiguration, GraphOptions graphOptions)
        {
            if (cassandraConfiguration == null)
            {
                throw new ArgumentNullException("cassandraConfiguration");
            }
            if (graphOptions == null)
            {
                throw new ArgumentNullException("graphOptions");
            }
            CassandraConfiguration = cassandraConfiguration;
            GraphOptions = graphOptions;
        }
    }

    internal class IdentityAddressTranslator : IAddressTranslator
    {
        public IPEndPoint Translate(IPEndPoint address)
        {
            return address;
        }
    }
}
