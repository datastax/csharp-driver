using System;
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
        private long _length;
        private ushort _listIndexPosition;
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
            get { return _length; }
        }

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

        public ListBackedStream(int capacity)
        {
            SetLength(capacity);
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
            throw new NotImplementedException();
        }

        public override void SetLength(long value)
        {
            _length = value;
        }

        public override void Write(byte[] buffer, int offset, int count)
        {
            Buffers.Add(Utils.SliceBuffer(buffer, offset, count));
            TotalLength += count;
            this._position = TotalLength;
        }
    }
}
