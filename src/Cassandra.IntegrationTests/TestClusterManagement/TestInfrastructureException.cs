//
//  Copyright (C) DataStax, Inc.
//
//  Please see the license for details:
//  http://www.datastax.com/terms/datastax-dse-driver-license-terms
//
using System;

namespace Cassandra.IntegrationTests.TestClusterManagement
{
    /// <summary>
    /// Represents an error on the infrastructure setup
    /// </summary>
    public class TestInfrastructureException: Exception
    {
        public TestInfrastructureException()
        {

        }

        public TestInfrastructureException(string message) : base(message)
        {

        }
    }
}
