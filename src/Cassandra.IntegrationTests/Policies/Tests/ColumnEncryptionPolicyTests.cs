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
using System.Linq;
using System.Threading.Tasks;
using Cassandra.IntegrationTests.TestBase;
using Cassandra.Tests;
using NUnit.Framework;

namespace Cassandra.IntegrationTests.Policies.Tests
{
    [TestFixture, Category(TestCategory.Short)]
    public class ColumnEncryptionPolicyTests : SharedClusterTest
    {
        private const string B64Key = "PJC7HnliwcxXw4FM8Ep3sX9NIL3R5CZnDvp8IyyCSlg=";
        private const string B64Iv = "f7gd72CubOjEmjrkTUX0uQ==";

        private static readonly AesColumnEncryptionPolicy.AesKeyAndIV KeyAndIv =
            new AesColumnEncryptionPolicy.AesKeyAndIV(Convert.FromBase64String(B64Key), Convert.FromBase64String(B64Iv));

        private static readonly AesColumnEncryptionPolicy.AesKeyAndIV KeyOnly =
            new AesColumnEncryptionPolicy.AesKeyAndIV(Convert.FromBase64String(B64Key));

        private IColumnEncryptionPolicy BuildColumnEncryptionPolicy(string tableName, AesColumnEncryptionPolicy.AesKeyAndIV key)
        {
            var p = new AesColumnEncryptionPolicy();
            p.AddColumn(KeyspaceName, tableName, "id", key, ColumnTypeCode.Uuid);
            p.AddColumn(KeyspaceName, tableName, "age", key, ColumnTypeCode.Int);
            p.AddColumn(KeyspaceName, tableName, "name", key, ColumnTypeCode.Text);
            return p;
        }

        private static void CreateEncryptedTable(ISession session, string name)
        {
            session.Execute($"CREATE TABLE {name} (id blob PRIMARY KEY, age blob, name blob, public_notes text)");
        }

        private void VerifyEncryptedUser(User user, ISession session, IColumnEncryptionPolicy policy, object key, Row r)
        {
            var idRaw = r["id"] as byte[];
            var nameRaw = r["name"] as byte[];
            var ageRaw = r["age"] as byte[];
            var publicNotes = r["public_notes"] as string;
            var serializer = ((Session)session).InternalRef.InternalCluster.GetControlConnection().Serializer.GetCurrentSerializer();
            Assert.NotNull(idRaw);
            Assert.NotNull(nameRaw);
            Assert.NotNull(ageRaw);
            Assert.NotNull(publicNotes);
            Assert.AreEqual(user.Id, serializer.Deserialize(policy.Decrypt(key, idRaw), ColumnTypeCode.Uuid, null));
            Assert.AreEqual(user.Name, serializer.Deserialize(policy.Decrypt(key, nameRaw), ColumnTypeCode.Text, null));
            Assert.AreEqual(user.Age, serializer.Deserialize(policy.Decrypt(key, ageRaw), ColumnTypeCode.Int, null));
            Assert.AreEqual(user.PublicNotes, publicNotes);
        }

        [Test]
        public async Task Should_InsertAndSelectEncryptedColumnsPartitionKeyOnly_PreparedStatements()
        {
            var tableName = TestUtils.GetUniqueTableName().ToLowerInvariant();
            var policy = BuildColumnEncryptionPolicy(tableName, KeyAndIv);
            var cluster = GetNewTemporaryCluster(builder => builder.WithColumnEncryptionPolicy(policy));
            var session = await cluster.ConnectAsync(KeyspaceName).ConfigureAwait(false);
            CreateEncryptedTable(session, tableName);
            var clusterNoEncryption = GetNewTemporaryCluster();
            var sessionNoEncryption = await clusterNoEncryption.ConnectAsync(KeyspaceName).ConfigureAwait(false);
            var insertQuery = $"INSERT INTO {tableName} (id, name, age, public_notes) VALUES (?, ?, ?, ?)";
            var selectQuery = $"SELECT * FROM {tableName} WHERE id = ?";
            var preparedInsert = await Session.PrepareAsync(insertQuery).ConfigureAwait(false);
            var preparedSelect = await Session.PrepareAsync(selectQuery).ConfigureAwait(false);

            var id = Guid.NewGuid();
            var newUser = new User
            {
                Id = id,
                Name = "User " + id,
                Age = id.GetHashCode() % 100,
                PublicNotes = "These are the public unencrypted notes for user " + id
            };

            var boundInsert = preparedInsert.Bind(newUser.Id, newUser.Name, newUser.Age, newUser.PublicNotes);
            var boundSelect = preparedSelect.Bind(newUser.Id);
            await session.ExecuteAsync(boundInsert).ConfigureAwait(false);
            var rs = await session.ExecuteAsync(boundSelect).ConfigureAwait(false);
            var users = rs.ToList();
            Assert.AreEqual(1, users.Count);
            var user = users[0];

            Assert.AreEqual(newUser.Id, user["id"]);
            Assert.AreEqual(newUser.Name, user["name"]);
            Assert.AreEqual(newUser.Age, user["age"]);
            Assert.AreEqual(newUser.PublicNotes, user["public_notes"]);

            rs = await sessionNoEncryption.ExecuteAsync(new SimpleStatement($"SELECT * FROM {tableName}")).ConfigureAwait(false);
            var allUsers = rs.ToList();

            Assert.AreEqual(1, allUsers.Count);
            VerifyEncryptedUser(newUser, session, policy, KeyAndIv, allUsers[0]);
        }

        [Test]
        [TestCassandraVersion(3, 11)]
        public async Task Should_InsertAndSelectEncryptedColumns_PreparedStatements()
        {
            var tableName = TestUtils.GetUniqueTableName().ToLowerInvariant();
            var policy = BuildColumnEncryptionPolicy(tableName, KeyAndIv);
            var cluster = GetNewTemporaryCluster(builder => builder.WithColumnEncryptionPolicy(policy));
            var session = await cluster.ConnectAsync(KeyspaceName).ConfigureAwait(false);
            CreateEncryptedTable(session, tableName);
            var clusterNoEncryption = GetNewTemporaryCluster();
            var sessionNoEncryption = await clusterNoEncryption.ConnectAsync(KeyspaceName).ConfigureAwait(false);
            var insertQuery = $"INSERT INTO {tableName} (id, name, age, public_notes) VALUES (?, ?, ?, ?)";
            var selectQuery = $"SELECT * FROM {tableName} WHERE id = ? AND name = ? AND age = ? ALLOW FILTERING";
            var preparedInsert = await Session.PrepareAsync(insertQuery).ConfigureAwait(false);
            var preparedSelect = await Session.PrepareAsync(selectQuery).ConfigureAwait(false);

            var id = Guid.NewGuid();
            var newUser = new User
            {
                Id = id,
                Name = "User " + id,
                Age = id.GetHashCode() % 100,
                PublicNotes = "These are the public unencrypted notes for user " + id
            };

            var boundInsert = preparedInsert.Bind(newUser.Id, newUser.Name, newUser.Age, newUser.PublicNotes);
            var boundSelect = preparedSelect.Bind(newUser.Id, newUser.Name, newUser.Age);
            await session.ExecuteAsync(boundInsert).ConfigureAwait(false);
            var rs = await session.ExecuteAsync(boundSelect).ConfigureAwait(false);
            var users = rs.ToList();
            Assert.AreEqual(1, users.Count);
            var user = users[0];

            Assert.AreEqual(newUser.Id, user["id"]);
            Assert.AreEqual(newUser.Name, user["name"]);
            Assert.AreEqual(newUser.Age, user["age"]);
            Assert.AreEqual(newUser.PublicNotes, user["public_notes"]);

            rs = await sessionNoEncryption.ExecuteAsync(new SimpleStatement($"SELECT * FROM {tableName}")).ConfigureAwait(false);
            var allUsers = rs.ToList();

            Assert.AreEqual(1, allUsers.Count);
            VerifyEncryptedUser(newUser, session, policy, KeyAndIv, allUsers[0]);
        }

        [Test]
        [TestCassandraVersion(3, 11)]
        public async Task Should_FailSelectEncryptedColumnsWithRandomIV_PreparedStatements()
        {
            var tableName = TestUtils.GetUniqueTableName().ToLowerInvariant();
            var policy = BuildColumnEncryptionPolicy(tableName, KeyOnly);
            var cluster = GetNewTemporaryCluster(builder => builder.WithColumnEncryptionPolicy(policy));
            var session = await cluster.ConnectAsync(KeyspaceName).ConfigureAwait(false);
            CreateEncryptedTable(session, tableName);
            var clusterNoEncryption = GetNewTemporaryCluster();
            var sessionNoEncryption = await clusterNoEncryption.ConnectAsync(KeyspaceName).ConfigureAwait(false);
            var insertQuery = $"INSERT INTO {tableName} (id, name, age, public_notes) VALUES (?, ?, ?, ?)";
            var selectQuery = $"SELECT * FROM {tableName} WHERE id = ? AND name = ? AND age = ? ALLOW FILTERING";
            var preparedInsert = await Session.PrepareAsync(insertQuery).ConfigureAwait(false);
            var preparedSelect = await Session.PrepareAsync(selectQuery).ConfigureAwait(false);

            var id = Guid.NewGuid();
            var newUser = new User
            {
                Id = id,
                Name = "User " + id,
                Age = id.GetHashCode() % 100,
                PublicNotes = "These are the public unencrypted notes for user " + id
            };

            var boundInsert = preparedInsert.Bind(newUser.Id, newUser.Name, newUser.Age, newUser.PublicNotes);
            var boundSelect = preparedSelect.Bind(newUser.Id, newUser.Name, newUser.Age);
            await session.ExecuteAsync(boundInsert).ConfigureAwait(false);
            var rs = await session.ExecuteAsync(boundSelect).ConfigureAwait(false);
            var users = rs.ToList();
            Assert.AreEqual(0, users.Count);

            rs = await sessionNoEncryption.ExecuteAsync(new SimpleStatement($"SELECT * FROM {tableName}")).ConfigureAwait(false);
            var allUsers = rs.ToList();

            Assert.AreEqual(1, allUsers.Count);
            VerifyEncryptedUser(newUser, session, policy, KeyOnly, allUsers[0]);
        }

        [Test]
        [TestCassandraVersion(3, 11)]
        public async Task Should_InsertAndSelectEncryptedColumns_SimpleStatements()
        {
            var tableName = TestUtils.GetUniqueTableName().ToLowerInvariant();
            var policy = BuildColumnEncryptionPolicy(tableName, KeyAndIv);
            var cluster = GetNewTemporaryCluster(builder => builder.WithColumnEncryptionPolicy(policy));
            var session = await cluster.ConnectAsync(KeyspaceName).ConfigureAwait(false);
            CreateEncryptedTable(session, tableName);
            var clusterNoEncryption = GetNewTemporaryCluster();
            var sessionNoEncryption = await clusterNoEncryption.ConnectAsync(KeyspaceName).ConfigureAwait(false);
            var insertQuery = $"INSERT INTO {tableName} (id, name, age, public_notes) VALUES (?, ?, ?, ?)";
            var selectQuery = $"SELECT * FROM {tableName} WHERE id = ? AND name = ? AND age = ? ALLOW FILTERING";

            var id = Guid.NewGuid();
            var newUser = new User
            {
                Id = id,
                Name = "User " + id,
                Age = id.GetHashCode() % 100,
                PublicNotes = "These are the public unencrypted notes for user " + id
            };

            await session.ExecuteAsync(new SimpleStatement(
                insertQuery, 
                new EncryptedValue(newUser.Id, KeyAndIv), 
                new EncryptedValue(newUser.Name, KeyAndIv), 
                new EncryptedValue(newUser.Age, KeyAndIv), 
                newUser.PublicNotes)).ConfigureAwait(false);
            var rs = await session.ExecuteAsync(new SimpleStatement(
                selectQuery, 
                new EncryptedValue(newUser.Id, KeyAndIv),
                new EncryptedValue(newUser.Name, KeyAndIv),
                new EncryptedValue(newUser.Age, KeyAndIv))).ConfigureAwait(false);

            var users = rs.ToList();
            Assert.AreEqual(1, users.Count);
            var user = users[0];

            Assert.AreEqual(newUser.Id, user["id"]);
            Assert.AreEqual(newUser.Name, user["name"]);
            Assert.AreEqual(newUser.Age, user["age"]);
            Assert.AreEqual(newUser.PublicNotes, user["public_notes"]);

            rs = await sessionNoEncryption.ExecuteAsync(new SimpleStatement($"SELECT * FROM {tableName}")).ConfigureAwait(false);
            var allUsers = rs.ToList();

            Assert.AreEqual(1, allUsers.Count);
            VerifyEncryptedUser(newUser, session, policy, KeyAndIv, allUsers[0]);
        }

        [Test]
        public async Task Should_InsertAndSelectEncryptedColumnsPartitionKeyOnly_SimpleStatements()
        {
            var tableName = TestUtils.GetUniqueTableName().ToLowerInvariant();
            var policy = BuildColumnEncryptionPolicy(tableName, KeyAndIv);
            var cluster = GetNewTemporaryCluster(builder => builder.WithColumnEncryptionPolicy(policy));
            var session = await cluster.ConnectAsync(KeyspaceName).ConfigureAwait(false);
            CreateEncryptedTable(session, tableName);
            var clusterNoEncryption = GetNewTemporaryCluster();
            var sessionNoEncryption = await clusterNoEncryption.ConnectAsync(KeyspaceName).ConfigureAwait(false);
            var insertQuery = $"INSERT INTO {tableName} (id, name, age, public_notes) VALUES (?, ?, ?, ?)";
            var selectQuery = $"SELECT * FROM {tableName} WHERE id = ?";

            var id = Guid.NewGuid();
            var newUser = new User
            {
                Id = id,
                Name = "User " + id,
                Age = id.GetHashCode() % 100,
                PublicNotes = "These are the public unencrypted notes for user " + id
            };

            await session.ExecuteAsync(new SimpleStatement(
                insertQuery,
                new EncryptedValue(newUser.Id, KeyAndIv),
                new EncryptedValue(newUser.Name, KeyAndIv),
                new EncryptedValue(newUser.Age, KeyAndIv),
                newUser.PublicNotes)).ConfigureAwait(false);
            var rs = await session.ExecuteAsync(new SimpleStatement(
                selectQuery,
                new EncryptedValue(newUser.Id, KeyAndIv))).ConfigureAwait(false);

            var users = rs.ToList();
            Assert.AreEqual(1, users.Count);
            var user = users[0];

            Assert.AreEqual(newUser.Id, user["id"]);
            Assert.AreEqual(newUser.Name, user["name"]);
            Assert.AreEqual(newUser.Age, user["age"]);
            Assert.AreEqual(newUser.PublicNotes, user["public_notes"]);

            rs = await sessionNoEncryption.ExecuteAsync(new SimpleStatement($"SELECT * FROM {tableName}")).ConfigureAwait(false);
            var allUsers = rs.ToList();

            Assert.AreEqual(1, allUsers.Count);
            VerifyEncryptedUser(newUser, session, policy, KeyAndIv, allUsers[0]);
        }

        private class User
        {
            public Guid Id { get; set; }

            public string Name { get; set; }

            public int Age { get; set; }

            public string PublicNotes { get; set; }
        }
    }
}