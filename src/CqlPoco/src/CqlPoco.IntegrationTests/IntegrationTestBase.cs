using CqlPoco.IntegrationTests.TestData;
using NUnit.Framework;

namespace CqlPoco.IntegrationTests
{
    /// <summary>
    /// Base class for integration tests that does common fixture/test setup and teardown.
    /// </summary>
    public abstract class IntegrationTestBase
    {
        protected ICqlClient CqlClient;

        [TestFixtureSetUp]
        public void FixtureSetup()
        {
            TestDataHelper.ResetTestData();
        }

        [SetUp]
        public void Setup()
        {
            CqlClient = CqlClientConfiguration.ForSession(SessionHelper.Session).BuildCqlClient();
        }
    }
}