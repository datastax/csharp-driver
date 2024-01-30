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
//

using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Cassandra;

namespace ColumnEncryptionExample
{
    /// <summary>
    /// Example that shows how to use column encryption with prepared statements and simple statements.
    /// Also shows when you should provide an IV or not for a particular column.
    /// </summary>
    internal class Program
    {
        // for the built in AES policy, this key has to have 128, 192 or 256 bit length
        private const string Base64EncryptionKey = "PJC7HnliwcxXw4FM8Ep3sX9NIL3R5CZnDvp8IyyCSlg=";

        // for the built in AES policy, the IV has to have a 16 byte length
        private const string Base64UserIdIv = "f7gd72CubOjEmjrkTUX0uQ==";

        private const string Keyspace = "examples";
        private const string Table = "encrypted_users";

        private static readonly string InsertCqlQuery = $"INSERT INTO {Keyspace}.{Table} (id, address, public_notes) VALUES (?, ?, ?)";
        private static readonly string SelectCqlQuery = $"SELECT * FROM {Keyspace}.{Table} WHERE id = ?";

        private ICluster _cluster;
        private ISession _session;
        private PreparedStatement _insertPs;
        private PreparedStatement _selectPs;
        private AesColumnEncryptionPolicy.AesKeyAndIV _userKeyAndIv;
        private AesColumnEncryptionPolicy.AesKeyAndIV _addressKey;

        private static void Main(string[] args)
        {
            new Program().MainAsync(args).GetAwaiter().GetResult();
        }

        private async Task MainAsync(string[] args)
        {
            // build column encryption policy
            var policy = new AesColumnEncryptionPolicy();

            // 'id' is in the primary key so we should ensure that the same id input results in the same server side encrypted value therefore we have to provide a static IV for this column.
            _userKeyAndIv = new AesColumnEncryptionPolicy.AesKeyAndIV(Convert.FromBase64String(Base64EncryptionKey), Convert.FromBase64String(Base64UserIdIv));
            policy.AddColumn(Keyspace, Table, "id", _userKeyAndIv, ColumnTypeCode.Uuid);

            // No need for the encrypted values of 'address' to be the same when the input is the same (e.g. not used in any WHERE clause, indexes or primary key)
            // so no need to provide IV, let the policy generate a new one per encryption operation (it's more secure).
            _addressKey = new AesColumnEncryptionPolicy.AesKeyAndIV(Convert.FromBase64String(Base64EncryptionKey));
            policy.AddColumn(Keyspace, Table, "address", _addressKey, ColumnTypeCode.Text);

            // build cluster
            _cluster =
                Cluster.Builder()
                    .AddContactPoint("127.0.0.1")
                    .WithColumnEncryptionPolicy(policy)
                    .Build();

            // create session
            _session = await _cluster.ConnectAsync().ConfigureAwait(false);
            try {

                // prepare schema
                await _session.ExecuteAsync(new SimpleStatement($"CREATE KEYSPACE IF NOT EXISTS {Keyspace} WITH replication = {{ 'class': 'SimpleStrategy', 'replication_factor': '1' }}")).ConfigureAwait(false);
                await _session.ExecuteAsync(new SimpleStatement($"CREATE TABLE IF NOT EXISTS {Keyspace}.{Table}(id blob, address blob, public_notes text, PRIMARY KEY(id))")).ConfigureAwait(false);

                await PreparedStatementsExample().ConfigureAwait(false);
                await SimpleStatementsExample().ConfigureAwait(false);

                Console.WriteLine("Press enter to exit.");
                Console.ReadLine();
            }
            finally
            {
                await _cluster.ShutdownAsync().ConfigureAwait(false);
            }
        }

        private async Task PreparedStatementsExample()
        {
            // prepare query
            _insertPs = await _session.PrepareAsync(Program.InsertCqlQuery).ConfigureAwait(false);
            _selectPs = await _session.PrepareAsync(Program.SelectCqlQuery).ConfigureAwait(false);

            var userId = Guid.NewGuid();
            var address = "Street X";
            var publicNotes = "Public notes 1.";

            var boundInsert = _insertPs.Bind(userId, address, publicNotes);
            await _session.ExecuteAsync(boundInsert).ConfigureAwait(false);

            var boundSelect = _selectPs.Bind(userId);
            var rs = await _session.ExecuteAsync(boundSelect).ConfigureAwait(false);
            var users = rs.ToList();
            if (users.Count != 1)
            {
                throw new Exception("could not retrieve the inserted user");
            }

            var user = users[0];
            Console.WriteLine($"User {user.GetValue<Guid>("id")} has address \"{user.GetValue<string>("address")}\" and public notes \"{user.GetValue<string>("public_notes")}\"");
        }
        
        private async Task SimpleStatementsExample()
        {
            var userId = Guid.NewGuid();
            var address = "Street Y";
            var publicNotes = "Public notes 2.";

            // using encrypted columns with SimpleStatements require the parameters to be wrapped with the EncryptedValue type
            var insert = new SimpleStatement(
                InsertCqlQuery, 
                new EncryptedValue(userId, _userKeyAndIv), 
                new EncryptedValue(address, _addressKey), 
                publicNotes);
            await _session.ExecuteAsync(insert).ConfigureAwait(false);

            var select = new SimpleStatement(SelectCqlQuery, new EncryptedValue(userId, _userKeyAndIv));
            var rs = await _session.ExecuteAsync(select).ConfigureAwait(false);

            var users = rs.ToList();
            if (users.Count != 1)
            {
                throw new Exception("could not retrieve the inserted user");
            }

            var user = users[0];
            Console.WriteLine($"User {user.GetValue<Guid>("id")} has address \"{user.GetValue<string>("address")}\" and public notes \"{user.GetValue<string>("public_notes")}\"");
        }
    }
}