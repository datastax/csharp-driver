//
//  Copyright (C) DataStax, Inc.
//
//  Please see the license for details:
//  http://www.datastax.com/terms/datastax-dse-driver-license-terms
//
using System;
using System.Collections.Generic;
using System.Linq;
using System.Reflection;
using Cassandra.Auth;
using NUnit.Framework;

namespace Cassandra.Tests
{
    public class ApiTests : BaseUnitTest
    {
        [Test]
        public void Cassandra_Auth_Namespace_Public_Test()
        {
            var types = GetTypesInNamespace("Cassandra.Auth", true);
            CollectionAssert.AreEquivalent(
                new[]
                {
#if !NETCORE
                    typeof(Cassandra.Auth.Sspi.SspiException), typeof(DseGssapiAuthProvider),
#endif
                    typeof(DsePlainTextAuthProvider)
                },
                types);
        }

        [Test]
        public void Cassandra_Single_Root_Namespace()
        {
            var assembly = typeof(IDseSession).GetTypeInfo().Assembly;
            var types = assembly.GetTypes();
            var set = new SortedSet<string>(
                types.Where(t => t.GetTypeInfo().IsPublic).Select(t => t.Namespace.Split('.')[0]));
            Assert.AreEqual(1, set.Count);
            Assert.AreEqual("Cassandra", set.First());
        }

        [Test]
        public void Cassandra_Exported_Namespaces()
        {
            var assembly = typeof(IDseSession).GetTypeInfo().Assembly;
            var types = assembly.GetTypes();
            var set = new SortedSet<string>(types.Where(t => t.GetTypeInfo().IsPublic).Select(t => t.Namespace));
            CollectionAssert.AreEqual(new[]
            {
                "Cassandra",
                "Cassandra.Auth",
#if !NETCORE
                "Cassandra.Auth.Sspi",
#endif
                "Cassandra.Data",
                "Cassandra.Data.Linq",
                "Cassandra.Geometry",
                "Cassandra.Graph",
                "Cassandra.Mapping",
                "Cassandra.Mapping.Attributes",
                "Cassandra.Mapping.TypeConversion",
                "Cassandra.Mapping.Utils",
                "Cassandra.Metrics",
                "Cassandra.Metrics.Abstractions",
                "Cassandra.Search",
                "Cassandra.Serialization"
            }, set);
        }

        private static IEnumerable<Type> GetTypesInNamespace(string nameSpace, bool recursive)
        {
            Func<string, bool> isMatch = n => n.StartsWith(nameSpace);
            if (!recursive)
            {
                isMatch = n => n == nameSpace;
            }
            var assembly = typeof (IDseSession).GetTypeInfo().Assembly;
            return assembly.GetTypes().Where(t => t.GetTypeInfo().IsPublic && isMatch(t.Namespace));
        }
    }
}
