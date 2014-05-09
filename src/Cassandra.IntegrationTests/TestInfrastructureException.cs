using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace Cassandra.IntegrationTests
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
