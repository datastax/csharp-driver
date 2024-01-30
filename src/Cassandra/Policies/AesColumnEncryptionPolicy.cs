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
using System.IO;
using System.Security.Cryptography;
// ReSharper disable InconsistentNaming

namespace Cassandra
{
    /// <summary>
    /// Implementation of <see cref="IColumnEncryptionPolicy"/> using the system's implementation of <see cref="Aes"/>.
    /// </summary>
    /// <remarks>
    /// <para>
    /// You can provide a Key and IV for each encrypted column or a key ONLY. If no IV is provided then a random one will be generated every time a value is encrypted. The IV will be stored with the value so prior knowledge of the IV is not needed for decryption.
    /// </para>
    /// <para>
    /// For columns that are used in WHERE clauses of SELECT statements you should always provide the same IV instead of relying on the randomly generated one because the value that is used for server side operations includes the IV.
    /// </para>
    /// <para>
    /// This implementation does not encrypt NULL values. NULL values will be written as empty byte arrays (without any IV).
    /// Any serialized value that is an empty byte array (like empty strings) will technically not be encrypted even though an IV will be stored next to it.
    /// </para>
    /// </remarks>
    public class AesColumnEncryptionPolicy : BaseColumnEncryptionPolicy<AesColumnEncryptionPolicy.AesKeyAndIV>
    {
        public const int IVLength = 16;

        // ReSharper disable once UseArrayEmptyMethod
        // Array.Empty is not available in net452
        private static readonly byte[] EmptyArray = new byte[0];

        /// <inheritdoc/>
        public override byte[] EncryptWithKey(AesKeyAndIV key, byte[] objBytes)
        {
            if (objBytes == null)
            {
                return EmptyArray;
            }
            using (var aes = Aes.Create())
            {
                aes.Mode = CipherMode.CBC;
                aes.Padding = PaddingMode.PKCS7;
                var IV = key.IV ?? aes.IV;
                using (var encryptor = aes.CreateEncryptor(key.Key, IV))
                {
                    using (var memoryStream = new MemoryStream())
                    {
                        memoryStream.Write(IV, 0, IV.Length);
                        using (var cryptoStream = new CryptoStream(memoryStream, encryptor, CryptoStreamMode.Write))
                        {
                            cryptoStream.Write(objBytes, 0, objBytes.Length);
                            cryptoStream.FlushFinalBlock();
                        }

                        return memoryStream.ToArray();
                    }
                }
            }
        }

        /// <inheritdoc/>
        public override byte[] DecryptWithKey(AesKeyAndIV key, byte[] encryptedBytes)
        {
            if (encryptedBytes == null || encryptedBytes.Length == 0)
            {
                return null;
            }
            using (var aes = Aes.Create())
            {
                aes.Mode = CipherMode.CBC;
                aes.Padding = PaddingMode.PKCS7;
                if (encryptedBytes == null || encryptedBytes.Length < IVLength)
                {
                    throw new DriverInternalError("could not decrypt column value because its size is " + encryptedBytes?.Length);
                }
                var IV = new byte[IVLength];
                Buffer.BlockCopy(encryptedBytes, 0, IV, 0, IVLength);

                using (var encryptor = aes.CreateDecryptor(key.Key, IV))
                {
                    using (var memoryStream = new MemoryStream())
                    {
                        using (var cryptoStream = new CryptoStream(memoryStream, encryptor, CryptoStreamMode.Write))
                        {
                            cryptoStream.Write(encryptedBytes, IVLength, encryptedBytes.Length - IVLength);
                            cryptoStream.FlushFinalBlock();
                        }

                        return memoryStream.ToArray();
                    }
                }

            }
        }

        // ReSharper disable once RedundantOverriddenMember
        // overriden method just to be able to reference it in API docs. 
        /// <inheritdoc/>
        public override void AddColumn(string ks, string table, string col, AesKeyAndIV key, ColumnTypeCode typeCode, IColumnInfo columnTypeInfo)
        {
            base.AddColumn(ks, table, col, key, typeCode, columnTypeInfo);
        }

        // ReSharper disable once RedundantOverriddenMember
        // overriden method just to be able to reference it in API docs. 
        /// <inheritdoc/>
        public override void AddColumn(string ks, string table, string col, AesKeyAndIV key, ColumnTypeCode typeCode)
        {
            base.AddColumn(ks, table, col, key, typeCode);
        }

        /// <summary>
        /// <para>
        /// This type contains a key and an IV to be used in AES encryption.
        /// The length of the IV has to be 16 bytes and the length of the key can be 128, 192 or 256 bits.
        /// </para>
        /// <para>
        /// The IV is optional. If no IV is provided, a new one will be randomly generated every time an encryption operation happens.
        /// <see cref="AesColumnEncryptionPolicy"/> encrypts values so that they contain the IV, i.e.,
        /// the driver can discard an IV as soon as it is used and it will still be able to decrypt any encrypted value regardless of the IV that was used.
        /// </para>
        /// <para>
        /// If you use an encrypted column in WHERE clauses of SELECT statements or any other server side operations that require the
        /// raw encrypted bytes of two equivalent "application" values to be the same then a "static" IV should be provided in this object
        /// instead of making the driver generate one for each encryption operation.
        /// E.g. let's say there is an "INSERT INTO table (X, Y) VALUES (?, ?)" statement and a "SELECT * FROM table where X = ?" statement where X is an encrypted column.
        /// If you don't provide an IV then an IV will be generated by the <see cref="AesColumnEncryptionPolicy"/> per encryption operation so the IV that will be used when
        /// encrypting the parameter for the INSERT statement will not be the same IV that is used when encrypting the parameter for the SELECT statement
        /// so these two values will not match on the server side and 0 rows will be returned.
        /// </para>
        /// </summary>
        public class AesKeyAndIV
        {
            /// <summary>
            /// AES Key (should be 128, 192 or 256 bits)
            /// </summary>
            public byte[] Key { get; }

            /// <summary>
            /// AES IV (should be 16 byte length) or null to make the policy generate one per encryption operation.
            /// </summary>
            public byte[] IV { get; }

            /// <summary>
            /// Constructor that doesn't allow you to provide the IV. If you need to provide a static IV, use <see cref="AesKeyAndIV(byte[],byte[])"/>.
            /// For information about whether you should provide an IV or not, see the documentation on this class <see cref="AesKeyAndIV"/>.
            /// </summary>
            public AesKeyAndIV(byte[] key) : this(key, null)
            {
            }

            /// <summary>
            /// Constructor that allows you to provide the IV. If you don't need to provide a static IV, use <see cref="AesKeyAndIV(byte[])"/>.
            /// For information about whether you should provide an IV or not, see the documentation on this class <see cref="AesKeyAndIV"/>.
            /// </summary>
            public AesKeyAndIV(byte[] key, byte[] iv)
            {
                Key = key;
                IV = iv;
                Validate(key, iv);
            }

            private static void Validate(byte[] key, byte[] iv)
            {
                if (key == null || key.Length == 0)
                {
                    throw new ArgumentException("invalid key size: " + (key?.Length ?? 0));
                }

                if (iv != null && iv.Length != IVLength)
                {
                    throw new ArgumentException("invalid IV size: " + iv.Length + ". Has to be 16 bytes.");
                } 
                try
                {
                    using (var aes = Aes.Create())
                    {
                        aes.Key = key;
                        if (iv != null)
                        {
                            aes.IV = iv;
                        }
                        using (var _ = aes.CreateEncryptor())
                        {
                        }
                    }
                }
                catch (Exception ex)
                {
                    throw new ArgumentException(
                        "Exception thrown while testing the provided key with .NET's AES provider (check inner exception). " +
                        "Make sure the key has a \"legal\" size for AES (it should be 128, 192 or 256 bits)", ex);
                }
            }
        }
    }
}
