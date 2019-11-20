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
            var assembly = typeof(ISession).GetTypeInfo().Assembly;
            var types = assembly.GetTypes();
            var set = new SortedSet<string>(
                types.Where(t => t.GetTypeInfo().IsPublic).Select(t => t.Namespace.Split('.')[0]));
            Assert.AreEqual(1, set.Count);
            Assert.AreEqual("Cassandra", set.First());
        }

        [Test]
        public void Cassandra_Exported_Namespaces()
        {
            var assembly = typeof(ISession).GetTypeInfo().Assembly;
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
                "Cassandra.DataStax.Graph",
                "Cassandra.DataStax.Search",
                "Cassandra.Geometry",
                "Cassandra.Mapping",
                "Cassandra.Mapping.Attributes",
                "Cassandra.Mapping.TypeConversion",
                "Cassandra.Mapping.Utils",
                "Cassandra.Metrics",
                "Cassandra.Metrics.Abstractions",
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
            var assembly = typeof (ISession).GetTypeInfo().Assembly;
            return assembly.GetTypes().Where(t => t.GetTypeInfo().IsPublic && isMatch(t.Namespace));
        }
    }
}
