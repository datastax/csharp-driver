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

using System.Reflection;
using System.Runtime.Versioning;
using NUnit.Framework;

namespace Cassandra.Tests
{
    [TestFixture]
    public class TargetTests
    {
#if NETCOREAPP
        [Test]
        public void Should_TargetNetstandard15_When_TestsTargetNetcore20()
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