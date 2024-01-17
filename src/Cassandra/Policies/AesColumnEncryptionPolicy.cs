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
    public class AesColumnEncryptionPolicy : BaseColumnEncryptionPolicy<AesColumnEncryptionPolicy.AesKeyAndIV>
    {
        public const int IVLength = 16;

        public override byte[] EncryptWithKey(AesKeyAndIV key, byte[] objBytes)
        {
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

        public override byte[] DecryptWithKey(AesKeyAndIV key, byte[] encryptedBytes)
        {
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

        public class AesKeyAndIV
        {
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
