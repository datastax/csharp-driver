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
    internal static class Decompressor
    {
        private const int LITERAL = 0;
        private const int MAX_INCREMENT_COPY_OVERFLOW = 20;

        private static short[] _opLookupTable = new short[]
        {
            0x0001, 0x0804, 0x1001, 0x2001, 0x0002, 0x0805, 0x1002, 0x2002,
            0x0003, 0x0806, 0x1003, 0x2003, 0x0004, 0x0807, 0x1004, 0x2004,
            0x0005, 0x0808, 0x1005, 0x2005, 0x0006, 0x0809, 0x1006, 0x2006,
            0x0007, 0x080a, 0x1007, 0x2007, 0x0008, 0x080b, 0x1008, 0x2008,
            0x0009, 0x0904, 0x1009, 0x2009, 0x000a, 0x0905, 0x100a, 0x200a,
            0x000b, 0x0906, 0x100b, 0x200b, 0x000c, 0x0907, 0x100c, 0x200c,
            0x000d, 0x0908, 0x100d, 0x200d, 0x000e, 0x0909, 0x100e, 0x200e,
            0x000f, 0x090a, 0x100f, 0x200f, 0x0010, 0x090b, 0x1010, 0x2010,
            0x0011, 0x0a04, 0x1011, 0x2011, 0x0012, 0x0a05, 0x1012, 0x2012,
            0x0013, 0x0a06, 0x1013, 0x2013, 0x0014, 0x0a07, 0x1014, 0x2014,
            0x0015, 0x0a08, 0x1015, 0x2015, 0x0016, 0x0a09, 0x1016, 0x2016,
            0x0017, 0x0a0a, 0x1017, 0x2017, 0x0018, 0x0a0b, 0x1018, 0x2018,
            0x0019, 0x0b04, 0x1019, 0x2019, 0x001a, 0x0b05, 0x101a, 0x201a,
            0x001b, 0x0b06, 0x101b, 0x201b, 0x001c, 0x0b07, 0x101c, 0x201c,
            0x001d, 0x0b08, 0x101d, 0x201d, 0x001e, 0x0b09, 0x101e, 0x201e,
            0x001f, 0x0b0a, 0x101f, 0x201f, 0x0020, 0x0b0b, 0x1020, 0x2020,
            0x0021, 0x0c04, 0x1021, 0x2021, 0x0022, 0x0c05, 0x1022, 0x2022,
            0x0023, 0x0c06, 0x1023, 0x2023, 0x0024, 0x0c07, 0x1024, 0x2024,
            0x0025, 0x0c08, 0x1025, 0x2025, 0x0026, 0x0c09, 0x1026, 0x2026,
            0x0027, 0x0c0a, 0x1027, 0x2027, 0x0028, 0x0c0b, 0x1028, 0x2028,
            0x0029, 0x0d04, 0x1029, 0x2029, 0x002a, 0x0d05, 0x102a, 0x202a,
            0x002b, 0x0d06, 0x102b, 0x202b, 0x002c, 0x0d07, 0x102c, 0x202c,
            0x002d, 0x0d08, 0x102d, 0x202d, 0x002e, 0x0d09, 0x102e, 0x202e,
            0x002f, 0x0d0a, 0x102f, 0x202f, 0x0030, 0x0d0b, 0x1030, 0x2030,
            0x0031, 0x0e04, 0x1031, 0x2031, 0x0032, 0x0e05, 0x1032, 0x2032,
            0x0033, 0x0e06, 0x1033, 0x2033, 0x0034, 0x0e07, 0x1034, 0x2034,
            0x0035, 0x0e08, 0x1035, 0x2035, 0x0036, 0x0e09, 0x1036, 0x2036,
            0x0037, 0x0e0a, 0x1037, 0x2037, 0x0038, 0x0e0b, 0x1038, 0x2038,
            0x0039, 0x0f04, 0x1039, 0x2039, 0x003a, 0x0f05, 0x103a, 0x203a,
            0x003b, 0x0f06, 0x103b, 0x203b, 0x003c, 0x0f07, 0x103c, 0x203c,
            0x0801, 0x0f08, 0x103d, 0x203d, 0x1001, 0x0f09, 0x103e, 0x203e,
            0x1801, 0x0f0a, 0x103f, 0x203f, 0x2001, 0x0f0b, 0x1040, 0x2040
        };

        private static int[] _wordmask;

        static Decompressor()
        {
            unchecked
            {
                _wordmask = new int[] { 0, 0xff, 0xffff, 0xffffff, (int)0xffffffff };
            }
        }

        public static int GetDecompressedLength(byte[] compressed, int compressedOffset)
        {
            return ReadUncompressedLength(compressed, compressedOffset)[0];
        }

        public static byte[] Decompress(byte[] compressed, int compressedOffset, int compressedSize)
        {
            int[] x = ReadUncompressedLength(compressed, compressedOffset);
            int expectedLength = x[0];
            compressedOffset += x[1];
            compressedSize -= x[1];

            byte[] uncompressed = new byte[expectedLength];
            int uncompressedSize = DecompressAllTags(
                compressed,
                compressedOffset,
                compressedSize,
                uncompressed,
                0
            );

            if (expectedLength != uncompressedSize)
            {
                throw new CorruptionException(
                    String.Format("Recorded length is {0} bytes but actual length after decompression is {1} bytes", expectedLength, uncompressedSize)
                );
            }

            return uncompressed;
        }

        public static int Decompress(byte[] compressed, int compressedOffset, int compressedSize, byte[] uncompressed, int uncompressedOffset)
        {
            int[] x = ReadUncompressedLength(compressed, compressedOffset);
            int expectedLength = x[0];
            compressedOffset += x[1];
            compressedSize -= x[1];

            int uncompressedSize = DecompressAllTags(
                compressed,
                compressedOffset,
                compressedSize,
                uncompressed,
                uncompressedOffset
            );

            if (expectedLength != uncompressedSize)
            {
                throw new CorruptionException(
                    String.Format("Recorded length is {0} bytes but actual length after decompression is {1} bytes", expectedLength, uncompressedSize)
                );
            }

            return expectedLength;
        }

        #region Private Methods

        private static int DecompressAllTags(byte[] input, int inputOffset, int inputSize, byte[] output, int outputOffset)
        {
            int outputLimit = output.Length;
            int inputLimit = inputOffset + inputSize;
            int outputIndex = outputOffset;
            int inputIndex = inputOffset;

            while (inputIndex < inputLimit - 5)
            {
                int opCode = DataHelper.LoadByte(input, inputIndex++);
                int entry = DataHelper.LookupShort(_opLookupTable, opCode);
                int trailerBytes = Extensions.BitwiseUnsignedRightShift(entry,11);
                int trailer = ReadTrailer(input, inputIndex, trailerBytes);

                inputIndex += Extensions.BitwiseUnsignedRightShift(entry,11);
                int length = entry & 0xff;

                if ((opCode & 0x3) == LITERAL)
                {
                    int literalLength = length + trailer;
                    CopyLiteral(input, inputIndex, output, outputIndex, literalLength);
                    inputIndex += literalLength;
                    outputIndex += literalLength;
                }
                else
                {
                    int copyOffset = entry & 0x700;
                    copyOffset += trailer;

                    int spaceLeft = outputLimit - outputIndex;
                    int srcIndex = outputIndex - copyOffset;
                    if (srcIndex < outputOffset)
                    {
                        throw new CorruptionException("Invalid copy offset for opcode starting at " + (inputIndex - trailerBytes - 1));
                    }

                    if (length <= 16 && copyOffset >= 8 && spaceLeft >= 16)
                    {
                        DataHelper.CopyLong(output, srcIndex, output, outputIndex);
                        DataHelper.CopyLong(output, srcIndex + 8, output, outputIndex + 8);
                    }
                    else if (spaceLeft >= length + MAX_INCREMENT_COPY_OVERFLOW)
                    {
                        IncrementalCopyFastPath(output, srcIndex, outputIndex, length);
                    }
                    else
                    {
                        IncrementalCopy(output, srcIndex, output, outputIndex, length);
                    }

                    outputIndex += length;
                }
            }

            while (inputIndex < inputLimit)
            {
                int[] result = DecompressTagSlow(input, inputIndex, output, outputLimit, outputOffset, outputIndex);
                inputIndex = result[0];
                outputIndex = result[1];
            }

            return outputIndex - outputOffset;
        }

        private static int[] DecompressTagSlow(byte[] input, int inputIndex, byte[] output, int outputLimit, int outputOffset, int outputIndex)
        {
            int opCode = DataHelper.LoadByte(input, inputIndex++);
            int entry = DataHelper.LookupShort(_opLookupTable, opCode);
            int trailerBytes = Extensions.BitwiseUnsignedRightShift(entry,11);

            int trailer = 0;
            switch (trailerBytes)
            {
                case 4:
                    trailer = (input[inputIndex + 3] & 0xff) << 24;
                    break;

                case 3:
                    trailer |= (input[inputIndex + 2] & 0xff) << 16;
                    break;

                case 2:
                    trailer |= (input[inputIndex + 1] & 0xff) << 8;
                    break;

                case 1:
                    trailer |= (input[inputIndex] & 0xff);
                    break;
            }

            inputIndex += trailerBytes;
            int length = entry & 0xff;

            if ((opCode & 0x3) == LITERAL)
            {
                int literalLength = length + trailer;
                CopyLiteral(input, inputIndex, output, outputIndex, literalLength);
                inputIndex += literalLength;
                outputIndex += literalLength;
            }
            else
            {
                int copyOffset = entry & 0x700;
                copyOffset += trailer;

                int spaceLeft = outputLimit - outputIndex;
                int srcIndex = outputIndex - copyOffset;

                if (srcIndex < outputOffset)
                {
                    throw new CorruptionException("Invalid copy offset for opcode starting at " + (inputIndex - trailerBytes - 1));
                }

                if (length <= 16 && copyOffset >= 8 && spaceLeft >= 16)
                {
                    DataHelper.CopyLong(output, srcIndex, output, outputIndex);
                    DataHelper.CopyLong(output, srcIndex + 8, output, outputIndex + 8);
                }
                else if (spaceLeft >= length + MAX_INCREMENT_COPY_OVERFLOW)
                {
                    IncrementalCopyFastPath(output, srcIndex, outputIndex, length);
                }
                else
                {
                    IncrementalCopy(output, srcIndex, output, outputIndex, length);
                }

                outputIndex += length;
            }

            return new int[] { inputIndex, outputIndex };
        }

        private static void CopyLiteral(byte[] input, int inputIndex, byte[] output, int outputIndex, int length)
        {
            Debug.Assert(length > 0);
            Debug.Assert(inputIndex >= 0);
            Debug.Assert(outputIndex >= 0);

            int spaceLeft = output.Length - outputIndex;
            int readableBytes = input.Length - inputIndex;

            if (readableBytes < length || spaceLeft < length)
            {
                throw new CorruptionException("Corrupt literal length");
            }

            if (length <= 16 && spaceLeft >= 16 && readableBytes >= 16)
            {
                DataHelper.CopyLong(input, inputIndex, output, outputIndex);
                DataHelper.CopyLong(input, inputIndex + 8, output, outputIndex + 8);
            }
            else
            {
                int fastLength;
                unchecked
                {
                    fastLength = length & (int)0xFFFFFFF8;
                }

                if (fastLength <= 64)
                {
                    for (int i = 0; i < fastLength; i += 8)
                    {
                        DataHelper.CopyLong(input, inputIndex + i, output, outputIndex + i);
                    }

                    int slowLength = length & 0x7;
                    for (int i = 0; i < slowLength; i += 1)
                    {
                        output[outputIndex + fastLength + i] = input[inputIndex + fastLength + i];
                    }
                }
                else
                {
                    DataHelper.CopyMemory(input, inputIndex, output, outputIndex, length);
                }
            }
        }

        private static void IncrementalCopy(byte[] src, int srcIndex, byte[] op, int opIndex, int length)
        {
            do
            {
                op[opIndex++] = src[srcIndex++];
            } while (--length > 0);
        }

        private static void IncrementalCopyFastPath(byte[] output, int inputIndex, int outputIndex, int length)
        {
            int copiedLength = 0;
            while ((outputIndex + copiedLength) - inputIndex < 8)
            {
                DataHelper.CopyLong(output, inputIndex, output, outputIndex + copiedLength);
                copiedLength += (outputIndex + copiedLength) - inputIndex;
            }

            for (int i = 0; i < length - copiedLength; i += 8)
            {
                DataHelper.CopyLong(output, inputIndex + i, output, outputIndex + copiedLength + i);
            }
        }

        private static int ReadTrailer(byte[] data, int index, int bytes)
        {
            return DataHelper.LoadInt(data, index) & _wordmask[bytes];
        }

        private static int[] ReadUncompressedLength(byte[] compressed, int compressedOffset)
        {
            int result;
            int bytesRead = 0;
            {
                int b = compressed[compressedOffset + bytesRead++] & 0xFF;
                result = b & 0x7f;
                if ((b & 0x80) != 0)
                {
                    b = compressed[compressedOffset + bytesRead++] & 0xFF;
                    result |= (b & 0x7f) << 7;
                    if ((b & 0x80) != 0)
                    {
                        b = compressed[compressedOffset + bytesRead++] & 0xFF;
                        result |= (b & 0x7f) << 14;
                        if ((b & 0x80) != 0)
                        {
                            b = compressed[compressedOffset + bytesRead++] & 0xFF;
                            result |= (b & 0x7f) << 21;
                            if ((b & 0x80) != 0)
                            {
                                b = compressed[compressedOffset + bytesRead++] & 0xFF;
                                result |= (b & 0x7f) << 28;
                                if ((b & 0x80) != 0)
                                {
                                    throw new CorruptionException("Last byte of compressed length integer has high bit set");
                                }
                            }
                        }
                    }
                }
            }

            return new int[] { result, bytesRead };
        }

        #endregion Private Methods
    }
}