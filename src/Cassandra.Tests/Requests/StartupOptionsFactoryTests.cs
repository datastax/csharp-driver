//
//       Copyright (C) DataStax Inc.
//
//    Licensed under the Apache License, Version 2.0 (the "License");
//    you may not use this file except in compliance with the License.
//    You may obtain a copy of the License at
//
//       http://www.apache.org/licenses/LICENSE-2.0
//
//    Unless required by applicable law or agreed to in writing, software
//    distributed under the License is distributed on an "AS IS" BASIS,
//    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//    See the License for the specific language governing permissions and
//    limitations under the License.
//

using System;
using System.Reflection;

using Cassandra.Helpers;
using Cassandra.Requests;

using NUnit.Framework;

namespace Cassandra.Tests.Requests
{
    [TestFixture]
    public class StartupOptionsFactoryTests
    {
        [Test]
        public void Should_ReturnCorrectProtocolStartupOptions_When_OptionsAreSet()
        {
            var factory = new StartupOptionsFactory();

            var options = factory.CreateStartupOptions(new ProtocolOptions().SetNoCompact(true).SetCompression(CompressionType.Snappy));

            Assert.AreEqual(5, options.Count);
            Assert.AreEqual("snappy", options["COMPRESSION"]);
            Assert.AreEqual("true", options["NO_COMPACT"]);
            var driverName = options["DRIVER_NAME"];
            Assert.True(driverName.Contains("DataStax") && driverName.Contains("C# Driver"), driverName);
            Assert.AreEqual("3.0.0", options["CQL_VERSION"]);

            var assemblyVersion = AssemblyHelpers.GetAssembly(typeof(Cluster)).GetCustomAttribute<AssemblyInformationalVersionAttribute>().InformationalVersion;
            Assert.AreEqual(assemblyVersion, options["DRIVER_VERSION"]);
            var indexOfVersionSuffix = assemblyVersion.IndexOf('-');
            var versionPrefix = indexOfVersionSuffix == -1 ? assemblyVersion : assemblyVersion.Substring(0, indexOfVersionSuffix);
            var version = Version.Parse(versionPrefix);
            Assert.Greater(version, new Version(1, 0));

            //// commented this so it doesn't break when version is bumped, tested this with and without suffix
            //// with suffix
            //Assert.AreEqual("3.8.0", versionPrefix);
            //Assert.AreEqual("3.8.0-alpha2", assemblyVersion);
            ////
            //// without suffix
            // Assert.AreEqual("3.8.0", versionPrefix);
            // Assert.AreEqual("3.8.0", assemblyVersion);
        }
    }
}