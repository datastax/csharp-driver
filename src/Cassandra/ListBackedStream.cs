//
//      Copyright (C) 2012-2014 DataStax Inc.
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

﻿using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;

namespace Cassandra
{
    /// <summary>
    /// Represents a readable stream that instead of being backed by a series of array of arrays of bytes.
    /// It prevents .NET to request an allocation in the Large Object Heap that can result in a OOO exception.
    /// It maintains the original sizes of the internal streams when writing
    /// </summary>
    internal class ListBackedStream : Stream
    {
        private int _listIndexPosition;
        private int _listBytePosition;
        private long _position;

        protected List<byte[]> Buffers { get; set; }

        public override bool CanRead
        {
            get { return true; }
        }

        public override bool CanSeek
        {
            get { return false; }
        }

        public override bool CanWrite
        {
            get { return true; }
        }

        public override long Length
        {
            get { return TotalLength; }
        }

        /// <summary>
        /// Determines that when writing, the buffer is kept by reference. 
        /// </summary>
        public bool KeepReferences { get; set; }

        /// <summary>
        /// Returns the length of the sum of the inner byte[] list
        /// </summary>
        public virtual long TotalLength { get; private set; }

        public override long Position
        {
            get
            {
                return _position;
            }
            set
            {
                _position = value;
                if (value == 0)
                {
                    _listIndexPosition = 0;
                    _listBytePosition = 0;
                }
            }
        }

        public ListBackedStream()
        {
            Buffers = new List<byte[]>();
        }

        public override void Flush()
        {

        }

        public override int Read(byte[] buffer, int offset, int count)
        {
            if (this._position + count > TotalLength)
            {
                throw new IndexOutOfRangeException("Trying to read pass the total size.");
            }
            int read = 0;
            int toRead = 0;
            while (read < count)
            {
                var readBuffer = Buffers[_listIndexPosition];
                toRead = readBuffer.Length - _listBytePosition;
                if (toRead + read > count)
                {
                    toRead = count - read;
                }
                Buffer.BlockCopy(readBuffer, _listBytePosition, buffer, offset, toRead);
                read += toRead;
                offset += toRead;
                _listBytePosition += toRead;
                if (_listBytePosition == Buffers[_listIndexPosition].Length)
                {
                    _listBytePosition = 0;
                    _listIndexPosition++;
                }
            }

            this._position += count;
            return count;
        }

        public override long Seek(long offset, SeekOrigin origin)
        {
            throw new NotSupportedException();
        }

        public override void SetLength(long value)
        {
            TotalLength = value;
        }

        public override void Write(byte[] buffer, int offset, int count)
        {
            if (KeepReferences && offset == 0 && count == buffer.Length)
            {
                //Keep the reference to the original buffer
                Buffers.Add(buffer);
            }
            else
            {
                Buffers.Add(Utils.SliceBuffer(buffer, offset, count));
            }
            TotalLength += count;
            this._position = TotalLength;
        }
    }
}
