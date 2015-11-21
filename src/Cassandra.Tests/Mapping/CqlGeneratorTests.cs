using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Cassandra.Mapping;
using Cassandra.Mapping.Statements;
using Cassandra.Tests.Mapping.Pocos;
using NUnit.Framework;

namespace Cassandra.Tests.Mapping
{
    public class CqlGeneratorTests : MappingTestBase
    {
        [Test]
        public void GenerateUpdate_Test()
        {
            var types = new Cassandra.Mapping.Utils.LookupKeyedCollection<Type, ITypeDefinition>(td => td.PocoType);
            types.Add(new Map<ExplicitColumnsUser>().TableName("users").PartitionKey(u => u.UserId).Column(u => u.UserAge, cm => cm.WithName("AGE")));
            var pocoFactory = new PocoDataFactory(types);
            var cqlGenerator = new CqlGenerator(pocoFactory);
            var cql = cqlGenerator.GenerateUpdate<ExplicitColumnsUser>();
            Assert.AreEqual("UPDATE users SET Name = ?, AGE = ? WHERE UserId = ?", cql);
        }

        [Test]
        public void GenerateUpdate_CaseSensitive_Test()
        {
            var types = new Cassandra.Mapping.Utils.LookupKeyedCollection<Type, ITypeDefinition>(td => td.PocoType);
            types.Add(new Map<ExplicitColumnsUser>()
                .TableName("users")
                .PartitionKey(u => u.UserId)
                .Column(u => u.UserAge, cm => cm.WithName("AGE"))
                .CaseSensitive());
            var pocoFactory = new PocoDataFactory(types);
            var cqlGenerator = new CqlGenerator(pocoFactory);
            var cql = cqlGenerator.GenerateUpdate<ExplicitColumnsUser>();
            Assert.AreEqual(@"UPDATE ""users"" SET ""Name"" = ?, ""AGE"" = ? WHERE ""UserId"" = ?", cql);
        }

        [Test]
        public void PrependUpdate_Test()
        {
            var types = new Cassandra.Mapping.Utils.LookupKeyedCollection<Type, ITypeDefinition>(td => td.PocoType);
            types.Add(new Map<ExplicitColumnsUser>().TableName("users").PartitionKey(u => u.UserId));
            var pocoFactory = new PocoDataFactory(types);
            var cqlGenerator = new CqlGenerator(pocoFactory);
            var cql = Cql.New("SET Name = ? WHERE UserId = ?", "New name", Guid.Empty);
            cqlGenerator.PrependUpdate<ExplicitColumnsUser>(cql);
            Assert.AreEqual("UPDATE users SET Name = ? WHERE UserId = ?", cql.Statement);
        }

        [Test]
        public void PrependUpdate_CaseSensitive_Test()
        {
            var types = new Cassandra.Mapping.Utils.LookupKeyedCollection<Type, ITypeDefinition>(td => td.PocoType);
            types.Add(new Map<ExplicitColumnsUser>()
                .TableName("users")
                .PartitionKey(u => u.UserId)
                .CaseSensitive());
            var pocoFactory = new PocoDataFactory(types);
            var cqlGenerator = new CqlGenerator(pocoFactory);
            var cql = Cql.New(@"SET ""Name"" = ? WHERE ""UserId"" = ?", "New name", Guid.Empty);
            cqlGenerator.PrependUpdate<ExplicitColumnsUser>(cql);
            Assert.AreEqual(@"UPDATE ""users"" SET ""Name"" = ? WHERE ""UserId"" = ?", cql.Statement);
        }

        [Test]
        public void AddSelect_Test()
        {
            var types = new Cassandra.Mapping.Utils.LookupKeyedCollection<Type, ITypeDefinition>(td => td.PocoType);
            types.Add(new Map<ExplicitColumnsUser>().TableName("users").PartitionKey(u => u.UserId).Column(u => u.UserAge, cm => cm.WithName("AGE")));
            var pocoFactory = new PocoDataFactory(types);
            var cqlGenerator = new CqlGenerator(pocoFactory);
            var cql = Cql.New("WHERE UserId = ?", Guid.Empty);
            cqlGenerator.AddSelect<ExplicitColumnsUser>(cql);
            Assert.AreEqual("SELECT UserId, Name, AGE FROM users WHERE UserId = ?", cql.Statement);
        }

        [Test]
        public void AddSelect_CaseSensitive_Test()
        {
            var types = new Cassandra.Mapping.Utils.LookupKeyedCollection<Type, ITypeDefinition>(td => td.PocoType);
            types.Add(new Map<ExplicitColumnsUser>()
                .TableName("users")
                .PartitionKey(u => u.UserId)
                .Column(u => u.UserAge, cm => cm.WithName("AGE"))
                .CaseSensitive());
            var pocoFactory = new PocoDataFactory(types);
            var cqlGenerator = new CqlGenerator(pocoFactory);
            var cql = Cql.New(@"WHERE ""UserId"" = ?", Guid.Empty);
            cqlGenerator.AddSelect<ExplicitColumnsUser>(cql);
            Assert.AreEqual(@"SELECT ""UserId"", ""Name"", ""AGE"" FROM ""users"" WHERE ""UserId"" = ?", cql.Statement);
        }

        [Test]
        public void GenerateDelete_Test()
        {
            var types = new Cassandra.Mapping.Utils.LookupKeyedCollection<Type, ITypeDefinition>(td => td.PocoType);
            types.Add(new Map<ExplicitColumnsUser>().TableName("USERS").PartitionKey(u => u.UserId));
            var pocoFactory = new PocoDataFactory(types);
            var cqlGenerator = new CqlGenerator(pocoFactory);
            var cql = cqlGenerator.GenerateDelete<ExplicitColumnsUser>();
            Assert.AreEqual("DELETE FROM USERS WHERE UserId = ?", cql);
        }

        [Test]
        public void GenerateDelete_CaseSensitive_Test()
        {
            var types = new Cassandra.Mapping.Utils.LookupKeyedCollection<Type, ITypeDefinition>(td => td.PocoType);
            types.Add(new Map<ExplicitColumnsUser>()
                .TableName("USERS")
                .PartitionKey("ID")
                .Column(u => u.UserId, cm => cm.WithName("ID"))
                .CaseSensitive());
            var pocoFactory = new PocoDataFactory(types);
            var cqlGenerator = new CqlGenerator(pocoFactory);
            var cql = cqlGenerator.GenerateDelete<ExplicitColumnsUser>();
            Assert.AreEqual(@"DELETE FROM ""USERS"" WHERE ""ID"" = ?", cql);
        }

        [Test]
        public void GenerateInsert_Test()
        {
            var types = new Cassandra.Mapping.Utils.LookupKeyedCollection<Type, ITypeDefinition>(td => td.PocoType);
            types.Add(new Map<ExplicitColumnsUser>()
                .TableName("USERS")
                .PartitionKey("ID")
                .Column(u => u.UserId, cm => cm.WithName("ID")));
            var pocoFactory = new PocoDataFactory(types);
            var cqlGenerator = new CqlGenerator(pocoFactory);
            var cql = cqlGenerator.GenerateInsert<ExplicitColumnsUser>();
            Assert.AreEqual(@"INSERT INTO USERS (ID, Name, UserAge) VALUES (?, ?, ?)", cql);
        }

        [Test]
        public void GenerateInsert_CaseSensitive_Test()
        {
            var types = new Cassandra.Mapping.Utils.LookupKeyedCollection<Type, ITypeDefinition>(td => td.PocoType);
            types.Add(new Map<ExplicitColumnsUser>()
                .TableName("USERS")
                .PartitionKey(u => u.UserId)
                .CaseSensitive());
            var pocoFactory = new PocoDataFactory(types);
            var cqlGenerator = new CqlGenerator(pocoFactory);
            var cql = cqlGenerator.GenerateInsert<ExplicitColumnsUser>();
            Assert.AreEqual(@"INSERT INTO ""USERS"" (""UserId"", ""Name"", ""UserAge"") VALUES (?, ?, ?)", cql);
        }

        [Test]
        public void GenerateInsertWithTtl_CaseSensitive_Test()
        {
            var types = new Cassandra.Mapping.Utils.LookupKeyedCollection<Type, ITypeDefinition>(td => td.PocoType);
            types.Add(new Map<ExplicitColumnsUser>()
                .TableName("USERS")
                .PartitionKey(u => u.UserId)
                .CaseSensitive());
            var pocoFactory = new PocoDataFactory(types);
            var cqlGenerator = new CqlGenerator(pocoFactory);
            var cql = cqlGenerator.GenerateInsert<ExplicitColumnsUser>();
            Assert.AreEqual(@"INSERT INTO ""USERS"" (""UserId"", ""Name"", ""UserAge"") VALUES (?, ?, ?)", cql);
        }
    }
}
