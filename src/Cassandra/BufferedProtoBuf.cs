using System;
using System.Threading;

namespace Cassandra
{
    internal class BufferedProtoBuf : IProtoBuf
    {
        readonly object _guard = new object();
        readonly byte[] _buffer;
        int _readPos;
        int _writePos;
        readonly IProtoBufComporessor _compressor;
        byte[] _decompressedBuffer;

        public BufferedProtoBuf(int bufferLength, IProtoBufComporessor compressor)
        {
            this._compressor = compressor;
            _buffer = new byte[bufferLength];
            _readPos = 0;
            _writePos = 0;
        }

        public void Write(byte[] buffer, int offset, int count)
        {
            lock (_guard)
            {
                if (_writePos == -1) throw new CassandraConnectionIOException();

                if (buffer != null)
                {
                    Buffer.BlockCopy(buffer, offset, this._buffer, _writePos, count);
                    _writePos += count;
                }
                else
                    _writePos = -1;
                Monitor.PulseAll(_guard);
            }
        }

        public void WriteByte(byte b)
        {
            lock (_guard)
            {
                if (_writePos == -1) throw new CassandraConnectionIOException();

                _buffer[_writePos] = b;
                _writePos++;
                Monitor.PulseAll(_guard);
            }
        }

        public void Read(byte[] buffer, int offset, int count)
        {
            if (count == 0) return;
            lock (_guard)
            {
                if (_writePos == -1) throw new CassandraConnectionIOException();

                if (_compressor != null)
                {
                    while (_writePos != -1 && _writePos < this._buffer.Length)
                        Monitor.Wait(_guard);

                    if (_writePos == -1) throw new CassandraConnectionIOException();

                    if (_decompressedBuffer == null)
                        _decompressedBuffer = _compressor.Decompress(this._buffer);
                    
                    if (count > _decompressedBuffer.Length - _readPos)
                        throw new DriverInternalError("Invalid decompression state");
                    
                    Buffer.BlockCopy(this._decompressedBuffer, _readPos, buffer, offset, count);
                }
                else
                {
                    while (_writePos != -1 && _readPos + count > _writePos)
                        Monitor.Wait(_guard);

                    if (_writePos == -1) throw new CassandraConnectionIOException();

                    Buffer.BlockCopy(this._buffer, _readPos, buffer, offset, count);
                }

                _readPos += count;
            }
        }

        public void Skip(int count)
        {
            lock (_guard)
            {
                if (_writePos == -1) throw new CassandraConnectionIOException();

                _readPos += count;
            }
        }
    }
}