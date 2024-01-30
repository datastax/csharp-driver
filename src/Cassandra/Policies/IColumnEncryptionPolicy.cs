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

namespace Cassandra
{
    /// <summary>
    /// <para>
    /// This policy is used by the driver to provide client side encryption capabilities. The only requirement is that the encrypted columns have the 'blob' type at schema level.
    /// Check https://docs.datastax.com/en/developer/csharp-driver/latest/features/column-encryption/ for more information about this feature.
    /// </para>
    /// <para>
    /// The driver provides an AES based implementation of this policy <see cref="AesColumnEncryptionPolicy"/>.
    /// </para>
    /// <para>
    /// You can implement your own policy by implementing a class that inherits the abstract class <see cref="BaseColumnEncryptionPolicy{TKey}"/>. This class provides some built-in functionality
    /// to manage the encrypted columns' metadata.
    /// </para>
    /// </summary>
    public interface IColumnEncryptionPolicy
    {
        /// <summary>
        /// Encrypt the specified bytes using the cryptography materials.
        /// This method is used by the driver internally before sending the parameters to the server.
        /// </summary>
        /// <returns>Encrypted data in a byte array. The returned byte array can't be 'null' because 'blob' types don't allow 'null' values.</returns>
        byte[] Encrypt(object key, byte[] objBytes);

        /// <summary>
        /// Decrypt the specified (encrypted) bytes using the cryptography materials.
        /// This method is used by the driver internally before providing the results to the application.
        /// </summary>
        /// <returns>Decrypted data in a byte array.</returns>
        byte[] Decrypt(object key, byte[] encryptedBytes);

        /// <summary>
        /// Retrieves the cryptography materials for the specified column. If the column is not encrypted then it should return 'null'.
        /// </summary>
        /// <param name="ks">Keyspace of this encrypted column's table.</param>
        /// <param name="table">Table of this encrypted column.</param>
        /// <param name="col">Name of this encrypted column at schema level.</param>
        ColumnEncryptionMetadata? GetColumnEncryptionMetadata(string ks, string table, string col);
    }
}
