using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Cassandra.Mapping;
using Cassandra.Mapping.Statements;
using Cassandra.Mapping.Utils;
using Cassandra.Tests.Mapping.Pocos;
using NUnit.Framework;

namespace Cassandra.Tests.Mapping
{
    public class CqlGeneratorTests : MappingTestBase
    {
        [Test]
        public void GenerateUpdate_Test()
        {
            var types = new LookupKeyedCollection<Type, ITypeDefinition>(td => td.PocoType);
            types.Add(new Map<ExplicitColumnsUser>().TableName("users").PartitionKey(u => u.UserId).Column(u => u.UserAge, cm => cm.WithName("AGE")));
            var pocoFactory = new PocoDataFactory(types);
            var cqlGenerator = new CqlGenerator(pocoFactory);
            var cql = cqlGenerator.GenerateUpdate<ExplicitColumnsUser>();
            Assert.AreEqual("UPDATE users SET Name = ?, AGE = ? WHERE UserId = ?", cql);
        }

        [Test]
        public void GenerateUpdate_CaseSensitive_Test()
        {
            var types = new LookupKeyedCollection<Type, ITypeDefinition>(td => td.PocoType);
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
        public void GenerateUpdate_Keyspace_Test()
        {
            var types = new LookupKeyedCollection<Type, ITypeDefinition>(td => td.PocoType);
            types.Add(new Map<ExplicitColumnsUser>()
                .TableName("users")
                .KeyspaceName("keyspace1")
                .PartitionKey(u => u.UserId)
                .Column(u => u.UserAge, cm => cm.WithName("AGE")));
            var pocoFactory = new PocoDataFactory(types);
            var cqlGenerator = new CqlGenerator(pocoFactory);
            var cql = cqlGenerator.GenerateUpdate<ExplicitColumnsUser>();
            Assert.AreEqual("UPDATE keyspace1.users SET Name = ?, AGE = ? WHERE UserId = ?", cql);
        }

        [Test]
        public void PrependUpdate_Test()
        {
            var types = new LookupKeyedCollection<Type, ITypeDefinition>(td => td.PocoType);
            types.Add(new Map<ExplicitColumnsUser>().TableName("users").PartitionKey(u => u.UserId));
            var pocoFactory = new PocoDataFactory(types);
            var cqlGenerator = new CqlGenerator(pocoFactory);
            var cql = Cql.New("SET Name = ? WHERE UserId = ?", "New name", Guid.Empty);
            cqlGenerator.PrependUpdate<ExplicitColumnsUser>(cql);
            Assert.AreEqual("UPDATE users SET Name = ? WHERE UserId = ?", cql.Statement);
        }

        [Test]
        public void PrependUpdate_Keyspace_Test()
        {
            var types = new LookupKeyedCollection<Type, ITypeDefinition>(td => td.PocoType);
            types.Add(new Map<ExplicitColumnsUser>()
                .TableName("users")
                .KeyspaceName("keyspace1")
                .PartitionKey(u => u.UserId));
            var pocoFactory = new PocoDataFactory(types);
            var cqlGenerator = new CqlGenerator(pocoFactory);
            var cql = Cql.New("SET Name = ? WHERE UserId = ?", "New name", Guid.Empty);
            cqlGenerator.PrependUpdate<ExplicitColumnsUser>(cql);
            Assert.AreEqual("UPDATE keyspace1.users SET Name = ? WHERE UserId = ?", cql.Statement);
        }

        [Test]
        public void PrependUpdate_CaseSensitive_Test()
        {
            var types = new LookupKeyedCollection<Type, ITypeDefinition>(td => td.PocoType);
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
            var types = new LookupKeyedCollection<Type, ITypeDefinition>(td => td.PocoType);
            types.Add(new Map<ExplicitColumnsUser>().TableName("users").PartitionKey(u => u.UserId).Column(u => u.UserAge, cm => cm.WithName("AGE")));
            var pocoFactory = new PocoDataFactory(types);
            var cqlGenerator = new CqlGenerator(pocoFactory);
            var cql = Cql.New("WHERE UserId = ?", Guid.Empty);
            cqlGenerator.AddSelect<ExplicitColumnsUser>(cql);
            Assert.AreEqual("SELECT UserId, Name, AGE FROM users WHERE UserId = ?", cql.Statement);
        }

        [Test]
        public void AddSelect_KeyspaceTest()
        {
            var types = new LookupKeyedCollection<Type, ITypeDefinition>(td => td.PocoType);
            types.Add(new Map<ExplicitColumnsUser>()
                .TableName("users")
                .KeyspaceName("keyspace1")
                .PartitionKey(u => u.UserId).Column(u => u.UserAge, cm => cm.WithName("AGE")));
            var pocoFactory = new PocoDataFactory(types);
            var cqlGenerator = new CqlGenerator(pocoFactory);
            var cql = Cql.New("WHERE UserId = ?", Guid.Empty);
            cqlGenerator.AddSelect<ExplicitColumnsUser>(cql);
            Assert.AreEqual("SELECT UserId, Name, AGE FROM keyspace1.users WHERE UserId = ?", cql.Statement);
        }

        [Test]
        public void AddSelect_CaseSensitive_Test()
        {
            var types = new LookupKeyedCollection<Type, ITypeDefinition>(td => td.PocoType);
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
            var types = new LookupKeyedCollection<Type, ITypeDefinition>(td => td.PocoType);
            types.Add(new Map<ExplicitColumnsUser>().TableName("USERS").PartitionKey(u => u.UserId));
            var pocoFactory = new PocoDataFactory(types);
            var cqlGenerator = new CqlGenerator(pocoFactory);
            var cql = cqlGenerator.GenerateDelete<ExplicitColumnsUser>();
            Assert.AreEqual("DELETE FROM USERS WHERE UserId = ?", cql);
        }

        [Test]
        public void GenerateDelete_Keyspace_Test()
        {
            var types = new LookupKeyedCollection<Type, ITypeDefinition>(td => td.PocoType);
            types.Add(new Map<ExplicitColumnsUser>()
                .TableName("USERS")
                .KeyspaceName("keyspace1")
                .PartitionKey(u => u.UserId));
            var pocoFactory = new PocoDataFactory(types);
            var cqlGenerator = new CqlGenerator(pocoFactory);
            var cql = cqlGenerator.GenerateDelete<ExplicitColumnsUser>();
            Assert.AreEqual("DELETE FROM keyspace1.USERS WHERE UserId = ?", cql);
        }

        [Test]
        public void GenerateDelete_CaseSensitive_Test()
        {
            var types = new LookupKeyedCollection<Type, ITypeDefinition>(td => td.PocoType);
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
            var types = new LookupKeyedCollection<Type, ITypeDefinition>(td => td.PocoType);
            types.Add(new Map<ExplicitColumnsUser>()
                .TableName("USERS")
                .PartitionKey("ID")
                .Column(u => u.UserId, cm => cm.WithName("ID")));
            var pocoFactory = new PocoDataFactory(types);
            var cqlGenerator = new CqlGenerator(pocoFactory);
            object[] queryParameters;
            var cql = cqlGenerator.GenerateInsert<ExplicitColumnsUser>(true, new object[0], out queryParameters);
            Assert.AreEqual(@"INSERT INTO USERS (ID, Name, UserAge) VALUES (?, ?, ?)", cql);
        }

        [Test]
        public void GenerateInsert_Keyspace_Test()
        {
            var types = new LookupKeyedCollection<Type, ITypeDefinition>(td => td.PocoType);
            types.Add(new Map<ExplicitColumnsUser>()
                .TableName("USERS")
                .KeyspaceName("keyspace1")
                .PartitionKey("ID")
                .Column(u => u.UserId, cm => cm.WithName("ID")));
            var pocoFactory = new PocoDataFactory(types);
            var cqlGenerator = new CqlGenerator(pocoFactory);
            object[] queryParameters;
            var cql = cqlGenerator.GenerateInsert<ExplicitColumnsUser>(true, new object[0], out queryParameters);
            Assert.AreEqual(@"INSERT INTO keyspace1.USERS (ID, Name, UserAge) VALUES (?, ?, ?)", cql);
        }

        [Test]
        public void GenerateInsert_Without_Nulls_Test()
        {
            var types = new LookupKeyedCollection<Type, ITypeDefinition>(td => td.PocoType);
            types.Add(new Map<ExplicitColumnsUser>()
                .TableName("USERS")
                .PartitionKey("ID")
                .Column(u => u.UserId, cm => cm.WithName("ID")));
            var pocoFactory = new PocoDataFactory(types);
            var cqlGenerator = new CqlGenerator(pocoFactory);
            var values = new object[] {Guid.NewGuid(), null, 100};
            object[] queryParameters;
            var cql = cqlGenerator.GenerateInsert<ExplicitColumnsUser>(false, values, out queryParameters);
            Assert.AreEqual(@"INSERT INTO USERS (ID, UserAge) VALUES (?, ?)", cql);
            CollectionAssert.AreEqual(values.Where(v => v != null), queryParameters);
            
            cql = cqlGenerator.GenerateInsert<ExplicitColumnsUser>(false, values, out queryParameters, true);
            Assert.AreEqual(@"INSERT INTO USERS (ID, UserAge) VALUES (?, ?) IF NOT EXISTS", cql);
            CollectionAssert.AreEqual(values.Where(v => v != null), queryParameters);
        }

        [Test]
        public void GenerateInsert_Without_Nulls_First_Value_Null_Test()
        {
            var types = new LookupKeyedCollection<Type, ITypeDefinition>(td => td.PocoType);
            types.Add(new Map<ExplicitColumnsUser>()
                .TableName("USERS")
                .PartitionKey("ID")
                .Column(u => u.UserId, cm => cm.WithName("ID")));
            var pocoFactory = new PocoDataFactory(types);
            var cqlGenerator = new CqlGenerator(pocoFactory);
            var values = new object[] { null, "name", 100 };
            object[] queryParameters;
            var cql = cqlGenerator.GenerateInsert<ExplicitColumnsUser>(false, values, out queryParameters);
            Assert.AreEqual(@"INSERT INTO USERS (Name, UserAge) VALUES (?, ?)", cql);
            CollectionAssert.AreEqual(values.Where(v => v != null), queryParameters);

            cql = cqlGenerator.GenerateInsert<ExplicitColumnsUser>(false, values, out queryParameters, true);
            Assert.AreEqual(@"INSERT INTO USERS (Name, UserAge) VALUES (?, ?) IF NOT EXISTS", cql);
            CollectionAssert.AreEqual(values.Where(v => v != null), queryParameters);
        }

        [Test]
        public void GenerateInsert_Without_Nulls_Should_Throw_When_Value_Is_Null_Test()
        {
            var types = new LookupKeyedCollection<Type, ITypeDefinition>(td => td.PocoType);
            types.Add(new Map<ExplicitColumnsUser>()
                .TableName("USERS")
                .PartitionKey("ID")
                .Column(u => u.UserId, cm => cm.WithName("ID")));
            var pocoFactory = new PocoDataFactory(types);
            var cqlGenerator = new CqlGenerator(pocoFactory);
            object[] queryParameters;
            Assert.Throws<ArgumentNullException>(() =>
                cqlGenerator.GenerateInsert<ExplicitColumnsUser>(false, null, out queryParameters));
        }

        [Test]
        public void GenerateInsert_Without_Nulls_Should_Throw_When_Value_Length_Dont_Match_Test()
        {
            var types = new LookupKeyedCollection<Type, ITypeDefinition>(td => td.PocoType);
            types.Add(new Map<ExplicitColumnsUser>()
                .TableName("USERS")
                .PartitionKey("ID")
                .Column(u => u.UserId, cm => cm.WithName("ID")));
            var pocoFactory = new PocoDataFactory(types);
            var cqlGenerator = new CqlGenerator(pocoFactory);
            object[] queryParameters;
            Assert.Throws<ArgumentException>(() =>
                cqlGenerator.GenerateInsert<ExplicitColumnsUser>(false, new object[] { Guid.NewGuid()}, out queryParameters));
        }

        [Test]
        public void GenerateInsert_CaseSensitive_Test()
        {
            var types = new LookupKeyedCollection<Type, ITypeDefinition>(td => td.PocoType);
            types.Add(new Map<ExplicitColumnsUser>()
                .TableName("USERS")
                .PartitionKey(u => u.UserId)
                .CaseSensitive());
            var pocoFactory = new PocoDataFactory(types);
            var cqlGenerator = new CqlGenerator(pocoFactory);
            object[] queryParameters;
            var cql = cqlGenerator.GenerateInsert<ExplicitColumnsUser>(true, new object[0], out queryParameters);
            Assert.AreEqual(@"INSERT INTO ""USERS"" (""UserId"", ""Name"", ""UserAge"") VALUES (?, ?, ?)", cql);
        }
    }
}
