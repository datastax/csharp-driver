using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Cassandra.Mapping;
using Cassandra.Mapping.FluentMapping;
using Cassandra.Mapping.Mapping;
using Cassandra.Mapping.Statements;
using Cassandra.Tests.Mapping.Pocos;
using NUnit.Framework;

namespace Cassandra.Tests.Mapping
{
    public class CqlGeneratorTests : MappingTestBase
    {
        [Test]
        public void GenerateUpdateTest()
        {
            var types = new Cassandra.Mapping.Utils.LookupKeyedCollection<Type, ITypeDefinition>(td => td.PocoType);
            types.Add(new Map<ExplicitColumnsUser>().TableName("users").PrimaryKey(u => u.UserId).Column(u => u.UserAge, cm => cm.WithName("AGE")));
            var pocoFactory = new PocoDataFactory(types);
            var cqlGenerator = new CqlGenerator(pocoFactory);
            var cql = cqlGenerator.GenerateUpdate<ExplicitColumnsUser>();
            Assert.AreEqual("UPDATE users SET Name = ?, AGE = ? WHERE UserId = ?", cql);
        }

        [Test]
        public void PrependUpdateTest()
        {
            var types = new Cassandra.Mapping.Utils.LookupKeyedCollection<Type, ITypeDefinition>(td => td.PocoType);
            types.Add(new Map<ExplicitColumnsUser>().TableName("users").PrimaryKey(u => u.UserId).Column(u => u.UserAge, cm => cm.WithName("AGE")));
            var pocoFactory = new PocoDataFactory(types);
            var cqlGenerator = new CqlGenerator(pocoFactory);
            var cql = Cql.New("SET Name = ? WHERE UserId = ?", "New name", Guid.Empty);
            cqlGenerator.PrependUpdate<ExplicitColumnsUser>(cql);
            Assert.AreEqual("UPDATE users SET Name = ? WHERE UserId = ?", cql.Statement);
        }
    }
}
