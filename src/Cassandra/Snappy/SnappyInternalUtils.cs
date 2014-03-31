/*
 * Copyright (C) 2011 the original author or authors.
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

// based on java version by Dain Sundstrom (https://github.com/dain)
// ported to C# from https://github.com/dain/snappy/blob/master/src/main/java/org/iq80/snappy/SnappyInternalUtils.java

using System;

namespace Snappy
{
    internal class SnappyInternalUtils
    {
        private static readonly IMemory _memory = new SlowMemory();

        private static readonly bool _hasUnsage = _memory.FastAccessSupported();

        private SnappyInternalUtils()
        {
        }

        private static bool Equals(byte[] left, int leftIndex, byte[] right, int rightIndex, int length)
        {
            CheckPositionIndexes(leftIndex, leftIndex + length, left.Length);
            CheckPositionIndexes(rightIndex, rightIndex + length, right.Length);

            for (int i = 0; i < length; i++)
            {
                if (left[leftIndex + i] != right[rightIndex + i])
                {
                    return false;
                }
            }
            return true;
        }

        public static int LookupShort(short[] data, int index)
        {
            return _memory.LookupShort(data, index);
        }

        public static int LoadByte(byte[] data, int index)
        {
            return _memory.LoadByte(data, index);
        }

        public static int LoadInt(byte[] data, int index)
        {
            return _memory.LoadInt(data, index);
        }

        public static void CopyLong(byte[] src, int srcIndex, byte[] dest, int destIndex)
        {
            _memory.CopyLong(src, srcIndex, dest, destIndex);
        }

        public static long LoadLong(byte[] data, int index)
        {
            return _memory.LoadLong(data, index);
        }

        public static void CopyMemory(byte[] input, int inputIndex, byte[] output, int outputIndex, int length)
        {
            _memory.CopyMemory(input, inputIndex, output, outputIndex, length);
        }

        //
        // Copied from Guava Preconditions
        //static <T> T checkNotNull(T reference, string errorMessageTemplate, Object... errorMessageArgs)
        //{
        //    if (reference == null) {
        //        // If either of these parameters is null, the right thing happens anyway
        //        throw new NullPointerException(string.format(errorMessageTemplate, errorMessageArgs));
        //    }
        //    return reference;
        //}

        public static void CheckArgument(bool expression, string errorMessageTemplate, object errorMessageArg0, object errorMessageArg1)
        {
            if (!expression)
            {
                throw new ArgumentException(string.Format(errorMessageTemplate, errorMessageArg0, errorMessageArg1));
            }
        }

        private static void CheckPositionIndexes(int start, int end, int size)
        {
            // Carefully optimized for execution by hotspot (explanatory comment above)
            if (start < 0 || end < start || end > size)
            {
                throw new IndexOutOfRangeException(BadPositionIndexes(start, end, size));
            }
        }

        public static string BadPositionIndexes(int start, int end, int size)
        {
            if (start < 0 || start > size)
            {
                return BadPositionIndex(start, size, "start index");
            }
            if (end < 0 || end > size)
            {
                return BadPositionIndex(end, size, "end index");
            }
            // end < start
            return string.Format("end index ({0}) must not be less than start index ({1})", end, start);
        }

        private static string BadPositionIndex(int index, int size, string desc)
        {
            if (index < 0)
            {
                return string.Format("{0} ({1}) must not be negative", desc, index);
            }
            if (size < 0)
            {
                throw new ArgumentException("negative size: " + size);
            }
            // index > size
            return string.Format("{0} ({1}) must not be greater than size (%s)", desc, index, size);
        }
    }
}