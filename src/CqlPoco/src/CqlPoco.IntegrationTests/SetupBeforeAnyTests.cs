using NUnit.Framework;

namespace CqlPoco.IntegrationTests
{
    /// <summary>
    /// Does one-time setup that needs to happen before any integration tests are run.
    /// </summary>
    [SetUpFixture]
    public class SetupBeforeAnyTests
    {
        [SetUp]
        public void Setup()
        {
            // Init the common session
            SessionHelper.InitSessionAndKeyspace();
        }

        [TearDown]
        public void TearDown()
        {
            // Remove keyspace we created for test run
            SessionHelper.RemoveKeyspace();
        }
    }
}
