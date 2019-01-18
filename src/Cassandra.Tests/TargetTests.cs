using System.Reflection;
using System.Runtime.Versioning;
using NUnit.Framework;

namespace Cassandra.Tests
{
    [TestFixture]
    public class TargetTests
    {
#if NETCORE
        [Test]
        public void Should_TargetNetstandard15_When_NetcoreIsDefined()
        {
            var framework = Assembly
                            .GetAssembly(typeof(ISession))?
                            .GetCustomAttribute<TargetFrameworkAttribute>()?
                            .FrameworkName;

            Assert.AreEqual(".NETStandard,Version=v1.5", framework);
        }
#endif
#if NETCOREAPP2_0
        [Test]
        public void Should_TargetNetstandard15_When_TestsTargetNetcore20()
        {
            var framework = Assembly
                            .GetAssembly(typeof(ISession))?
                            .GetCustomAttribute<TargetFrameworkAttribute>()?
                            .FrameworkName;

            Assert.AreEqual(".NETStandard,Version=v1.5", framework);
        }
#elif NET452
        [Test]
        public void Should_TargetNet45_When_TestsTargetNet452()
        {
            var framework = Assembly
                            .GetAssembly(typeof(ISession))?
                            .GetCustomAttribute<TargetFrameworkAttribute>()?
                            .FrameworkName;

            Assert.AreEqual(".NETFramework,Version=v4.5", framework);
        }

#elif NETCOREAPP2_1
        [Test]
        public void Should_TargetNetstandard20_When_TestsTargetNetcore21()
        {
            var framework = Assembly
                            .GetAssembly(typeof(ISession))?
                            .GetCustomAttribute<TargetFrameworkAttribute>()?
                            .FrameworkName;

            Assert.AreEqual(".NETStandard,Version=v2.0", framework);
        }
#else
        [Test]
        public void Should_FailTest_When_TestsTargetDifferentTarget()
        {
            Assert.Fail("Something changed in the test project targets. "+
                    "Please review these tests to make sure the change is intended "+
                    "and if so please add new tests for the new targets.")
        }
#endif
    }
}