using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using NUnit.Framework;
using System.Reflection;
#pragma warning disable 618

namespace Cassandra.Tests
{
    /// <summary>
    /// Checks that the methods and properties that should be marked as obsolete, actually are
    /// </summary>
    [TestFixture]
    public class ObsoletedMemberTests
    {
        [Test]
        public void SimpleStatement_Bind_Obsolete()
        {
            var method = typeof(SimpleStatement).GetTypeInfo().GetMethod("Bind", BindingFlags.Public | BindingFlags.Instance);
            Assert.NotNull(method);
            Assert.AreEqual(1, method.GetCustomAttributes(typeof(ObsoleteAttribute), true).Count());
        }

        [Test]
        public void ISession_WaitForSchemaAgreement_Obsolete()
        {
            var methods = typeof(ISession).GetMethods(BindingFlags.Public | BindingFlags.Instance)
                .Where(m => m.Name == "WaitForSchemaAgreement")
                .ToArray();
            Assert.AreEqual(2, methods.Length);
            foreach (var m in methods)
            {
                Assert.AreEqual(1, m.GetCustomAttributes(typeof(ObsoleteAttribute), true).Count());
            }
        }

        [Test]
        public void RowSet_Dispose_Obsolete()
        {
            var method = typeof(RowSet).GetMethod("Dispose", BindingFlags.Public | BindingFlags.Instance);
            Assert.NotNull(method);
            Assert.AreEqual(1, method.GetCustomAttributes(typeof(ObsoleteAttribute), true).Count());
        }

        [Test]
        public void Linq_Attributes_Obsolete()
        {

            var linqAttributes = typeof (Cassandra.Data.Linq.TableAttribute).GetTypeInfo().Assembly.GetTypes()
                .Select(t => t.GetTypeInfo())
                .Where(t => t.IsPublic && t.IsSubclassOf(typeof(Attribute)) && t.Namespace == "Cassandra.Data.Linq")
                .ToArray();
            Assert.Greater(linqAttributes.Length, 5);
            foreach (var attr in linqAttributes)
            {
                Assert.AreEqual(1, attr.GetCustomAttributes(typeof (ObsoleteAttribute), true).Count(), "Type not obsolete " + attr.FullName);
            }
        }
    }
}
