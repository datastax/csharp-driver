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
using Dse.Auth;
using NUnit.Framework;

namespace Dse.Test.Unit
{
    public class ApiTests : BaseUnitTest
    {
        [Test]
        public void Dse_Auth_Namespace_Public_Test()
        {
            var types = GetTypesInNamespace("Dse.Auth", true);
            CollectionAssert.AreEquivalent(
                new[]
                {
#if !NETCORE
                    typeof(Dse.Auth.Sspi.SspiException), typeof(DseGssapiAuthProvider),
#endif
                    typeof(DsePlainTextAuthProvider)
                },
                types);
        }

        [Test]
        public void Dse_Single_Root_Namespace()
        {
            var assembly = typeof(IDseSession).GetTypeInfo().Assembly;
            var types = assembly.GetTypes();
            var set = new SortedSet<string>(
                types.Where(t => t.GetTypeInfo().IsPublic).Select(t => t.Namespace.Split('.')[0]));
            Assert.AreEqual(1, set.Count);
            Assert.AreEqual("Dse", set.First());
        }

        [Test]
        public void Dse_Exported_Namespaces()
        {
            var assembly = typeof(IDseSession).GetTypeInfo().Assembly;
            var types = assembly.GetTypes();
            var set = new SortedSet<string>(types.Where(t => t.GetTypeInfo().IsPublic).Select(t => t.Namespace));
            CollectionAssert.AreEqual(new[]
            {
                "Dse",
                "Dse.Auth",
#if !NETCORE
                "Dse.Auth.Sspi",
#endif
                "Dse.Data",
                "Dse.Data.Linq",
                "Dse.Geometry",
                "Dse.Graph",
                "Dse.Mapping",
                "Dse.Mapping.Attributes",
                "Dse.Mapping.TypeConversion",
                "Dse.Mapping.Utils",
                "Dse.Search",
                "Dse.Serialization"
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
