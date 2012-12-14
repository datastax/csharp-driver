/*
 * Copyright (C) 2012 the original author or authors.
 * See the notice.md file distributed with this work for additional
 * information regarding copyright ownership.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

using System;

namespace Snappy
{
    internal static class Snappy
    {
        public static int MaxCompressedLength(int dataLength)
        {
            return Compressor.MaxCompressedLength(dataLength);
        }

        public static int Compress(byte[] uncompressed, int uncompressedOffset, int uncompressedLength, byte[] compressed, int compressedOffset)
        {
            return Compressor.Compress(uncompressed, uncompressedOffset, uncompressedLength, compressed, compressedOffset);
        }

        public static byte[] Compress(byte[] data)
        {
            byte[] compressedOut = new byte[Compressor.MaxCompressedLength(data.Length)];
            int compressedSize = Compress(data, 0, data.Length, compressedOut, 0);
            byte[] result = new byte[compressedSize];
            Array.Copy(compressedOut, result, compressedSize);
            return result;
        }

        public static int GetDecompressedLength(byte[] compressed, int compressedOffset)
        {
            return Decompressor.GetDecompressedLength(compressed, compressedOffset);
        }

        public static byte[] Decompress(byte[] compressed, int compressedOffset, int compressedSize)
        {
            return Decompressor.Decompress(compressed, compressedOffset, compressedSize);
        }

        public static int Decompress(byte[] compressed, int compressedOffset, int compressedSize, byte[] uncompressed, int uncompressedOffset)
        {
            return Decompressor.Decompress(compressed, compressedOffset, compressedSize, uncompressed, uncompressedOffset);
        }
    }
}