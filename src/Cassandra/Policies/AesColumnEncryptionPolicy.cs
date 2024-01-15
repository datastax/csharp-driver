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
using System.Collections.Concurrent;
using System.IO;
using System.Security.Cryptography;

namespace Cassandra
{
    public class AesColumnEncryptionPolicy : BaseColumnEncryptionPolicy<byte[]>
    {
        public override byte[] Encrypt(string ks, string table, string col, byte[] objBytes)
        {
            if (!GetColData(ks, table, col, out var colData))
            {
                throw new ArgumentException(
                    $"Can't encrypt because the column {col} from table {ks}.{table} was not added to this column encryption policy.");
            }
            using (var aes = Aes.Create())
            {
                aes.Mode = CipherMode.CBC;
                aes.Padding = PaddingMode.PKCS7;
                using (var encryptor = aes.CreateEncryptor(colData.Key, aes.IV))
                {
                    using (var memoryStream = new MemoryStream())
                    {
                        using (var cryptoStream = new CryptoStream(memoryStream, encryptor, CryptoStreamMode.Write))
                        {
                            cryptoStream.Write(aes.IV, 0, aes.IV.Length);
                            cryptoStream.Write(objBytes, 0, objBytes.Length);
                        }

                        return memoryStream.ToArray();
                    }
                }
            }
        }

        public override byte[] Decrypt(string ks, string table, string col, byte[] encryptedBytes)
        {
            if (!GetColData(ks, table, col, out var colData))
            {
                throw new ArgumentException(
                    $"Can't decrypt because the column {col} from table {ks}.{table} was not added to this column encryption policy.");
            }
            using (var aes = Aes.Create())
            {
                aes.Mode = CipherMode.CBC;
                aes.Padding = PaddingMode.PKCS7;
                using (var memoryStream = new MemoryStream(encryptedBytes))
                {
                    const int ivLength = 16;
                    var IV = new byte[ivLength];
                    var bytesRead = memoryStream.Read(IV, 0, ivLength);
                    if (bytesRead != ivLength)
                    {
                        throw new DriverInternalError("could not read IV bytes from the buffer");
                    }
                    using (var encryptor = aes.CreateDecryptor(colData.Key, IV))
                    {
                        using (var cryptoStream = new CryptoStream(memoryStream, encryptor, CryptoStreamMode.Read))
                        {
                            using (var decryptedMemoryStream = new MemoryStream())
                            {
                                cryptoStream.CopyTo(decryptedMemoryStream);
                                return decryptedMemoryStream.ToArray();
                            }
                        }
                    }
                }
            }
        }

        protected override void ValidateKey(byte[] key)
        {
            if (key == null || key.Length == 0)
            {
                throw new ArgumentException("invalid key size: " + (key?.Length ?? 0));
            }
            try
            {
                using (var aes = Aes.Create())
                {
                    aes.Key = key;
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
