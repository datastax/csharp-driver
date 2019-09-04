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

using Dse.Mapping;
using Dse.Mapping.Utils;
using Moq;
using NUnit.Framework;

namespace Dse.Test.Unit.Mapping.Utils
{
    [TestFixture]
    public class CqlIdentifierHelperTests
    {
        [TestCase("\"select\"", "select", false)]
        [TestCase("\"select\"", "select", true)]
        [TestCase("\"\"", "", false)]
        [TestCase("\"\"", "", true)]
        [TestCase("\" \"", " ", false)]
        [TestCase("\" \"", " ", true)]
        [TestCase("notreserved", "notreserved", false)]
        [TestCase("\"notreserved\"", "notreserved", true)]
        [Test]
        public void Should_EscapeIdentifier(string expected, string identifier, bool caseSensitive)
        {
            var pocoData = Mock.Of<IPocoData>();
            Mock.Get(pocoData).SetupGet(m => m.CaseSensitive).Returns(caseSensitive);
            var target = new CqlIdentifierHelper();

            var act = target.EscapeIdentifierIfNecessary(pocoData, identifier);

            Assert.AreEqual(expected, act);
        }
        
        [TestCase("ks.\"select\"", "ks", "select", false)]
        [TestCase("\"ks\".\"select\"", "ks", "select", true)]
        [TestCase("\"select\".test", "select", "test", false)]
        [TestCase("\"select\".\"test\"", "select", "test", true)]
        [TestCase("\" \".\" \"", " ", " ", false)]
        [TestCase("\" \".\" \"", " ", " ", true)]
        [TestCase("\"\".\"\"", "", "", false)]
        [TestCase("\"\".\"\"", "", "", true)]
        [TestCase("test", null, "test", false)]
        [TestCase("\"test\"", null, "test", true)]
        [Test]
        public void Should_EscapeTableName(string expected, string keyspace, string tableName, bool caseSensitive)
        {
            var pocoData = Mock.Of<IPocoData>();
            Mock.Get(pocoData).SetupGet(m => m.CaseSensitive).Returns(caseSensitive);
            var target = new CqlIdentifierHelper();

            var act = target.EscapeTableNameIfNecessary(pocoData, keyspace, tableName);

            Assert.AreEqual(expected, act);
        }
    }
}