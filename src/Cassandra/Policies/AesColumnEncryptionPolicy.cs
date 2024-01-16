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
    public class AesColumnEncryptionPolicy : BaseColumnEncryptionPolicy<AesColumnEncryptionPolicy.AesKeyAndIV>
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
                var IV = colData.Key.IV ?? aes.IV;
                using (var encryptor = aes.CreateEncryptor(colData.Key.Key, IV))
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

        private void CryptoOp(MemoryStream memoryStream, ICryptoTransform transform, byte[] data)
        {
            using (var cryptoStream = new CryptoStream(memoryStream, transform, CryptoStreamMode.Write))
            {
                cryptoStream.Write(data, 0, data.Length);
                cryptoStream.FlushFinalBlock();
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
                const int ivLength = 16;
                if (encryptedBytes == null || encryptedBytes.Length < ivLength)
                {
                    throw new DriverInternalError("could not decrypt column value because its size is " + encryptedBytes?.Length);
                }
                var IV = new byte[ivLength];
                Buffer.BlockCopy(encryptedBytes, 0, IV, 0, ivLength);

                //using (var memoryStream = new MemoryStream(encryptedBytes))
                //{
                //    var IV = new byte[ivLength];
                //    var bytesRead = memoryStream.Read(IV, 0, ivLength);
                //    if (bytesRead != ivLength)
                //    {
                //        throw new DriverInternalError("could not read IV bytes from the buffer");
                //    }
                //    using (var encryptor = aes.CreateDecryptor(colData.Key, IV))
                //    {
                //        using (var cryptoStream = new CryptoStream(memoryStream, encryptor, CryptoStreamMode.Read))
                //        {
                //            using (var decryptedMemoryStream = new MemoryStream())
                //            {
                //                cryptoStream.CopyTo(decryptedMemoryStream);
                //                return decryptedMemoryStream.ToArray();
                //            }
                //        }
                //    }
                //}

                using (var encryptor = aes.CreateDecryptor(colData.Key.Key, IV))
                {
                    using (var memoryStream = new MemoryStream())
                    {
                        using (var cryptoStream = new CryptoStream(memoryStream, encryptor, CryptoStreamMode.Write))
                        {
                            cryptoStream.Write(encryptedBytes, ivLength, encryptedBytes.Length - ivLength);
                            cryptoStream.FlushFinalBlock();
                        }

                        return memoryStream.ToArray();
                    }
                }

            }
        }

        public class AesKeyAndIV
        {
            public const int IVLength = 16;

            public byte[] Key { get; }

            public byte[] IV { get; }

            public AesKeyAndIV(byte[] key) : this(key, null)
            {
            }

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
