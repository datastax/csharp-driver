// 
//       Copyright (C) 2019 DataStax, Inc.
// 
//     Please see the license for details:
//     http://www.datastax.com/terms/datastax-dse-driver-license-terms
// 

using System;
using System.Reflection;
using Dse.Helpers;
using Dse.Requests;
using NUnit.Framework;

namespace Dse.Test.Unit.Requests
{
    [TestFixture]
    public class DseStartupOptionsFactoryTests
    {
        [Test]
        public void Should_ReturnCorrectDseSpecificStartupOptions_When_OptionsAreSet()
        {
            var clusterId = Guid.NewGuid();
            var appName = "app123";
            var appVersion = "1.2.0";
            var factory = new DseStartupOptionsFactory(clusterId, appVersion, appName);

            var options = factory.CreateStartupOptions(new ProtocolOptions().SetNoCompact(true).SetCompression(CompressionType.Snappy));

            Assert.AreEqual(8, options.Count);
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

            Assert.AreEqual(appName, options["APPLICATION_NAME"]);
            Assert.AreEqual(appVersion, options["APPLICATION_VERSION"]);
            Assert.AreEqual(clusterId.ToString(), options["CLIENT_ID"]);
        }

        [Test]
        public void Should_NotReturnOptions_When_OptionsAreNull()
        {
            var clusterId = Guid.NewGuid();
            var factory = new DseStartupOptionsFactory(clusterId, null, null);

            var options = factory.CreateStartupOptions(new ProtocolOptions().SetNoCompact(true).SetCompression(CompressionType.Snappy));

            Assert.AreEqual(6, options.Count);
            Assert.IsFalse(options.ContainsKey("APPLICATION_NAME"));
            Assert.IsFalse(options.ContainsKey("APPLICATION_VERSION"));
        }
    }
}