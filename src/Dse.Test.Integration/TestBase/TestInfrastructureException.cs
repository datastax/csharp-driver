using System;

namespace Dse.Test.Integration.TestBase
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
