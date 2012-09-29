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
using System.Threading;

namespace Snappy
{
    internal class BufferManager
    {
        private const int MIN_ENCODING_BUFFER = 4000;
        private const int MIN_OUTPUT_BUFFER = 8000;

        [ThreadStatic]
        private static WeakReference _bufferReference = null;

        private byte[] _inputBuffer;
        private byte[] _outputBuffer;
        private byte[] _decodingBuffer;
        private byte[] _encodingBuffer;
        private short[] _encodingHash;

        public static BufferManager GetInstance()
        {
            BufferManager manager;
            if (_bufferReference == null)
            {
                manager = null;
            }
            else
            {
                manager = _bufferReference.Target as BufferManager;
            }

            if (manager == null)
            {
                manager = new BufferManager();
                _bufferReference = new WeakReference(manager, false);
            }

            return manager;
        }

        public byte[] AllocateEncodingBuffer(int minSize)
        {
            byte[] buffer = _encodingBuffer;
            if (buffer == null || buffer.Length < minSize)
            {
                buffer = new byte[Math.Max(minSize, MIN_ENCODING_BUFFER)];
            }
            else
            {
                _encodingBuffer = null;
            }

            return buffer;
        }

        public void ReleaseEncodingBuffer(byte[] buffer)
        {
            if (_encodingBuffer == null || buffer.Length > _encodingBuffer.Length)
            {
                _encodingBuffer = buffer;
            }
        }

        public byte[] AllocateOutputBuffer(int size)
        {
            byte[] buffer = _outputBuffer;
            if (buffer == null || buffer.Length < size)
            {
                buffer = new byte[Math.Max(size, MIN_OUTPUT_BUFFER)];
            }
            else
            {
                _outputBuffer = null;
            }

            return buffer;
        }

        public void ReleaseOutputBuffer(byte[] buffer)
        {
            if (_outputBuffer == null || (buffer != null && buffer.Length > _outputBuffer.Length))
            {
                _outputBuffer = buffer;
            }
        }

        public short[] AllocateEncodingHash(int size)
        {
            short[] buffer = _encodingHash;
            if (buffer == null || buffer.Length < size)
            {
                buffer = new short[size];
            }
            else
            {
                _encodingHash = null;
            }

            return buffer;
        }

        public void ReleaseEncodingHash(short[] buffer)
        {
            if (_encodingHash == null || (buffer != null && buffer.Length > _encodingHash.Length))
            {
                _encodingHash = buffer;
            }
        }

        public byte[] AllocateInputBuffer(int size)
        {
            byte[] buffer = _inputBuffer;
            if (buffer == null || buffer.Length < size)
            {
                buffer = new byte[Math.Max(size, MIN_OUTPUT_BUFFER)];
            }
            else
            {
                _inputBuffer = null;
            }

            return buffer;
        }

        public void ReleaseInputBuffer(byte[] buffer)
        {
            if (_inputBuffer == null || (buffer != null && buffer.Length > _inputBuffer.Length))
            {
                _inputBuffer = buffer;
            }
        }

        public byte[] AllocateDecodingBuffer(int size)
        {
            byte[] buffer = _decodingBuffer;
            if (buffer == null || buffer.Length < size)
            {
                buffer = new byte[size];
            }
            else
            {
                _decodingBuffer = null;
            }

            return buffer;
        }

        public void ReleaseDecodingBuffer(byte[] buffer)
        {
            if (_decodingBuffer == null || (buffer != null && buffer.Length > _decodingBuffer.Length))
            {
                _decodingBuffer = buffer;
            }
        }
    }
}