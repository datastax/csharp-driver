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
using System.Diagnostics;

namespace Snappy
{
    internal static class Compressor
    {
        private const int LITERAL = 0;
        private const int COPY_1_BYTE_OFFSET = 1;
        private const int COPY_2_BYTE_OFFSET = 2;
        private const int BLOCK_LOG = 15;
        private const int INPUT_MARGIN_BYTES = 15;
        private const int MAX_HASH_TABLE_BITS = 14;

        private static readonly int[] _zeroLookup =
        {
            32, 0, 1, 26, 2, 23, 27, 0, 3, 16, 24, 30, 28, 11, 0, 13, 4, 7, 17,
            0, 25, 22, 31, 15, 29, 10, 12, 6, 0, 21, 14, 9, 5, 20, 8, 19, 18
        };

        private static int _blockSize = 1 << BLOCK_LOG;
        private static int _maxHashTableSize = 1 << MAX_HASH_TABLE_BITS;

        public static int MaxCompressedLength(int dataLength)
        {
            return 32 + dataLength + dataLength / 6;
        }

        public static int Compress(byte[] uncompressed, int uncompressedOffset, int uncompressedLength, byte[] compressed, int compressedOffset)
        {
            int compressedIndex = WriteUncompressedLength(compressed, compressedOffset, uncompressedLength);
            int hashTableSize = GetHashTableSize(uncompressedLength);

            var buffer = BufferManager.GetInstance();
            short[] table = buffer.AllocateEncodingHash(hashTableSize);
            for (int read = 0; read < uncompressedLength; read += _blockSize)
            {
                for (int i = 0; i < table.Length; i++)
                {
                    table[i] = (short)0;
                }

                compressedIndex = CompressFragment(
                    uncompressed,
                    uncompressedOffset + read,
                    Math.Min(uncompressedLength - read, _blockSize),
                    compressed,
                    compressedIndex,
                    table);
            }

            buffer.ReleaseEncodingHash(table);
            return compressedIndex - compressedOffset;
        }

        #region Private Methods

        private static int CompressFragment(byte[] input, int inputOffset, int inputSize, byte[] output, int outputIndex, short[] table)
        {
            Debug.Assert(inputSize <= _blockSize);

            int inputIndex = inputOffset;
            int inputEndIndex = inputOffset + inputSize;
            int hashTableSize = GetHashTableSize(inputSize);
            int shift = 32 - Log2Floor(hashTableSize);

            int nextEmitIndex = inputIndex;
            if (inputSize >= INPUT_MARGIN_BYTES)
            {
                int inputLimit = inputOffset + inputSize - INPUT_MARGIN_BYTES;
                while (inputIndex <= inputLimit)
                {
                    Debug.Assert(nextEmitIndex <= inputIndex);

                    int skip = 32;
                    int[] candidateResult = FindCandidate(input, inputIndex, inputLimit, inputOffset, shift, table, skip);
                    inputIndex = candidateResult[0];
                    int candidateIndex = candidateResult[1];
                    skip = candidateResult[2];
                    if (inputIndex + BytesBetweenHashLookups(skip) > inputLimit)
                    {
                        break;
                    }

                    Debug.Assert(nextEmitIndex + 16 <= inputEndIndex);

                    outputIndex = EmitLiteral(output, outputIndex, input, nextEmitIndex, inputIndex - nextEmitIndex, true);
                    int[] indexes = EmitCopies(input, inputOffset, inputSize, inputIndex, output, outputIndex, table, shift, candidateIndex);
                    inputIndex = indexes[0];
                    outputIndex = indexes[1];
                    nextEmitIndex = inputIndex;
                }
            }

            if (nextEmitIndex < inputEndIndex)
            {
                outputIndex = EmitLiteral(output, outputIndex, input, nextEmitIndex, inputEndIndex - nextEmitIndex, false);
            }

            return outputIndex;
        }

        private static int[] FindCandidate(byte[] input, int inputIndex, int inputLimit, int inputOffset, int shift, short[] table, int skip)
        {
            int candidateIndex = 0;
            for (inputIndex += 1; inputIndex + BytesBetweenHashLookups(skip) <= inputLimit; inputIndex += BytesBetweenHashLookups(skip++))
            {
                int currentInt = DataHelper.LoadInt(input, inputIndex);
                int hash = HashBytes(currentInt, shift);
                candidateIndex = inputOffset + table[hash];

                Debug.Assert(candidateIndex >= 0);
                Debug.Assert(candidateIndex < inputIndex);

                table[hash] = (short)(inputIndex - inputOffset);
                if (currentInt == DataHelper.LoadInt(input, candidateIndex))
                {
                    break;
                }
            }

            return new int[] { inputIndex, candidateIndex, skip };
        }

        private static int GetHashTableSize(int inputSize)
        {
            Debug.Assert(_maxHashTableSize >= 256);

            int hashTableSize = 256;
            while (hashTableSize < _maxHashTableSize && hashTableSize < inputSize)
            {
                hashTableSize <<= 1;
            }

            Debug.Assert(hashTableSize <= _maxHashTableSize);
            return hashTableSize;
        }

        private static int[] EmitCopies(
            byte[] input,
            int inputOffset,
            int inputSize,
            int inputIndex,
            byte[] output,
            int outputIndex,
            short[] table,
            int shift,
            int candidateIndex)
        {
            int inputBytes;
            do
            {
                int matched = 4 + FindMatchLength(input, candidateIndex + 4, input, inputIndex + 4, inputOffset + inputSize);
                int offset = inputIndex - candidateIndex;

                inputIndex += matched;
                outputIndex = EmitCopy(output, outputIndex, offset, matched);
                if (inputIndex >= inputOffset + inputSize - INPUT_MARGIN_BYTES)
                {
                    return new int[] { inputIndex, outputIndex };
                }

                int previous = DataHelper.LoadInt(input, inputIndex - 1);
                inputBytes = DataHelper.LoadInt(input, inputIndex);

                int previousHash = HashBytes(previous, shift);
                table[previousHash] = (short)(inputIndex - inputOffset - 1);

                int currentHash = HashBytes(inputBytes, shift);
                candidateIndex = inputOffset + table[currentHash];
                table[currentHash] = (short)(inputIndex - inputOffset);
            } while (inputBytes == DataHelper.LoadInt(input, candidateIndex));

            return new int[] { inputIndex, outputIndex };
        }

        private static int EmitLiteral(byte[] output, int outputIndex, byte[] literal, int literalIndex, int length, bool allowFastPath)
        {
            int n = length - 1;
            if (n < 60)
            {
                output[outputIndex++] = (byte)(LITERAL | n << 2);
                if (allowFastPath && length <= 16)
                {
                    DataHelper.CopyLong(literal, literalIndex, output, outputIndex);
                    DataHelper.CopyLong(literal, literalIndex + 8, output, outputIndex + 8);
                    outputIndex += length;
                    return outputIndex;
                }
            }
            else if (n < (1 << 8))
            {
                output[outputIndex++] = (byte)(LITERAL | 59 + 1 << 2);
                output[outputIndex++] = (byte)n;
            }
            else if (n < (1 << 16))
            {
                output[outputIndex++] = (byte)(LITERAL | 59 + 2 << 2);
                output[outputIndex++] = (byte)n;
                output[outputIndex++] = (byte)Extensions.BitwiseUnsignedRightShift(n,8);
            }
            else if (n < (1 << 24))
            {
                output[outputIndex++] = (byte)(LITERAL | 59 + 3 << 2);
                output[outputIndex++] = (byte)n;
                output[outputIndex++] = (byte)Extensions.BitwiseUnsignedRightShift(n,8);
                output[outputIndex++] = (byte)Extensions.BitwiseUnsignedRightShift(n,16);
            }
            else
            {
                output[outputIndex++] = (byte)(LITERAL | 59 + 4 << 2);
                output[outputIndex++] = (byte)n;
                output[outputIndex++] = (byte)Extensions.BitwiseUnsignedRightShift(n,8);
                output[outputIndex++] = (byte)Extensions.BitwiseUnsignedRightShift(n,16);
                output[outputIndex++] = (byte)Extensions.BitwiseUnsignedRightShift(n,24);
            }

            Array.Copy(literal, literalIndex, output, outputIndex, length);
            outputIndex += length;
            return outputIndex;
        }

        private static int EmitCopy(byte[] output, int outputIndex, int offset, int length)
        {
            while (length >= 68)
            {
                outputIndex = EmitCopyLessThan64(output, outputIndex, offset, 64);
                length -= 64;
            }

            if (length > 64)
            {
                outputIndex = EmitCopyLessThan64(output, outputIndex, offset, 60);
                length -= 60;
            }

            outputIndex = EmitCopyLessThan64(output, outputIndex, offset, length);
            return outputIndex;
        }

        private static int EmitCopyLessThan64(byte[] output, int outputIndex, int offset, int length)
        {
            Debug.Assert(offset >= 0);
            Debug.Assert(length <= 64);
            Debug.Assert(length >= 4);
            Debug.Assert(offset < 65536);

            if ((length < 12) && (offset < 2048))
            {
                int lenMinus4 = length - 4;
                Debug.Assert(lenMinus4 < 8);
                output[outputIndex++] = (byte)(COPY_1_BYTE_OFFSET | ((lenMinus4) << 2) | (Extensions.BitwiseUnsignedRightShift(offset,8) << 5));
                output[outputIndex++] = (byte)offset;
            }
            else
            {
                output[outputIndex++] = (byte)(COPY_2_BYTE_OFFSET | ((length - 1) << 2));
                output[outputIndex++] = (byte)offset;
                output[outputIndex++] = (byte)Extensions.BitwiseUnsignedRightShift(offset,8);
            }

            return outputIndex;
        }

        private static int FindMatchLength(byte[] s1, int s1Index, byte[] s2, int s2Index, int s2Limit)
        {
            Debug.Assert(s2Limit >= s2Index);

            int length = s2Limit - s2Index;
            for (int matched = 0; matched < length; matched++)
            {
                if (s1[s1Index + matched] != s2[s2Index + matched])
                {
                    return matched;
                }
            }

            return length;
        }

        private static int WriteUncompressedLength(byte[] compressed, int compressedOffset, int uncompressedLength)
        {
            int highBitMask = 0x80;
            if (uncompressedLength < (1 << 7) && uncompressedLength >= 0)
            {
                compressed[compressedOffset++] = (byte)uncompressedLength;
            }
            else if (uncompressedLength < (1 << 14) && uncompressedLength > 0)
            {
                compressed[compressedOffset++] = (byte)(uncompressedLength | highBitMask);
                compressed[compressedOffset++] = (byte)Extensions.BitwiseUnsignedRightShift(uncompressedLength,7);
            }
            else if (uncompressedLength < (1 << 21) && uncompressedLength > 0)
            {
                compressed[compressedOffset++] = (byte)(uncompressedLength | highBitMask);
                compressed[compressedOffset++] = (byte)(Extensions.BitwiseUnsignedRightShift(uncompressedLength,7) | highBitMask);
                compressed[compressedOffset++] = (byte)Extensions.BitwiseUnsignedRightShift(uncompressedLength,14);
            }
            else if (uncompressedLength < (1 << 28) && uncompressedLength > 0)
            {
                compressed[compressedOffset++] = (byte)(uncompressedLength | highBitMask);
                compressed[compressedOffset++] = (byte)(Extensions.BitwiseUnsignedRightShift(uncompressedLength,7) | highBitMask);
                compressed[compressedOffset++] = (byte)(Extensions.BitwiseUnsignedRightShift(uncompressedLength,14) | highBitMask);
                compressed[compressedOffset++] = (byte)Extensions.BitwiseUnsignedRightShift(uncompressedLength,21);
            }
            else
            {
                compressed[compressedOffset++] = (byte)(uncompressedLength | highBitMask);
                compressed[compressedOffset++] = (byte)(Extensions.BitwiseUnsignedRightShift(uncompressedLength,7) | highBitMask);
                compressed[compressedOffset++] = (byte)(Extensions.BitwiseUnsignedRightShift(uncompressedLength,14) | highBitMask);
                compressed[compressedOffset++] = (byte)(Extensions.BitwiseUnsignedRightShift(uncompressedLength,21) | highBitMask);
                compressed[compressedOffset++] = (byte)Extensions.BitwiseUnsignedRightShift(uncompressedLength,28);
            }

            return compressedOffset;
        }

        private static int BytesBetweenHashLookups(int skip)
        {
            return Extensions.BitwiseUnsignedRightShift(skip,5);
        }

        private static int HashBytes(int bytes, int shift)
        {
            int multiplier = 0x1e35a7bd;
            return Extensions.BitwiseUnsignedRightShift(bytes * multiplier,shift);
        }

        private static int Log2Floor(int n)
        {
            return n == 0 ? -1 : 31 ^ GetNumberOfLeadingZeros(n);
        }

        private static int GetNumberOfLeadingZeros(int n)
        {
            int leadingZeros = 0;
            while (n != 0)
            {
                n = n >> 1;
                leadingZeros++;
            }

            return (32 - leadingZeros);
        }

        private static int GetNumberOfTrailingZeros(int n)
        {
            return _zeroLookup[(n & -n) % 37];
        }

        #endregion Private Methods
    }
}