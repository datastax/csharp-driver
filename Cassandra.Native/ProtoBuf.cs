using System;
using System.IO;
using System.Threading;
using System.Diagnostics;

namespace Cassandra
{

    internal interface IProtoBuf
    {
        void Write(byte[] buffer, int offset, int count);
        void WriteByte(byte b);
        void Read(byte[] buffer, int offset, int count);
        void Skip(int count);
    }

    internal class StreamProtoBuf : IProtoBuf
    {
        readonly Stream _stream;
        bool _ioError = false;
        IProtoBufComporessor _compressor;
        public StreamProtoBuf(Stream stream, IProtoBufComporessor compressor)
        {
            this._stream = stream;
            this._compressor = compressor;
        }

        public void Write(byte[] buffer, int offset, int count)
        {

            if (buffer == null)
                _ioError = true;
            else
            {
                if (_ioError) throw new CassandraConncectionIOException();

                try
                {
                    lock(_stream)
                        _stream.Write(buffer, offset, count);
                }
                catch (IOException ex)
                {
                    Debug.WriteLine(ex.Message, "StreamProtoBuf.Write");
                    _ioError = true;
                    throw new CassandraConncectionIOException(ex);
                }
            }
        }

        public void WriteByte(byte b)
        {
            if (_ioError) throw new CassandraConncectionIOException();
            lock (_stream)
                _stream.WriteByte(b);
        }

        public void Read(byte[] buffer, int offset, int count)
        {
            if (count == 0) return;

            if (_ioError) throw new CassandraConncectionIOException();

            int curOffset = offset;
            while (true)
            {
                try
                {
                    int redl;
                    lock (_stream)
                    {
                        redl = _stream.Read(buffer, curOffset, count - curOffset - offset);
                    }
                    if (redl == 0)
                    {
                        throw new CassandraConncectionIOException();
                    }
                    else if (redl == count - curOffset - offset)
                    {
                        return;
                    }
                    else
                    {
                        curOffset += redl;
                    }
                }
                catch (IOException ex)
                {
                    Debug.WriteLine(ex.Message, "StreamProtoBuf.Read");
                    _ioError = true;
                    throw new CassandraConncectionIOException(ex);
                }
            }
        }

        readonly byte[] _trashBuf = new byte[10 * 1024];

        public void Skip(int count)
        {
            if (_ioError) return;

            int curOffset = 0;
            while (true)
            {
                try
                {

                    int redl;
                    lock (_stream)
                    {
                        redl = _stream.Read(_trashBuf, curOffset, count - curOffset);
                    }
                    if (redl == 0)
                    {
                        throw new CassandraConncectionIOException();
                    }
                    else if (redl == count - curOffset)
                    {
                        return;
                    }
                    else
                    {
                        curOffset += redl;
                    }
                }
                catch (IOException ex)
                {
                    Debug.WriteLine(ex.Message, "StreamProtoBuf.Skip");
                    _ioError = true;
                    throw new CassandraConncectionIOException(ex);
                }
            }
        }

    }

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
                if (_writePos == -1) throw new CassandraConncectionIOException();

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
                if (_writePos == -1) throw new CassandraConncectionIOException();

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
                if (_writePos == -1) throw new CassandraConncectionIOException();

                if (_compressor != null)
                {
                    while (_writePos != -1 && _writePos < this._buffer.Length)
                        Monitor.Wait(_guard);

                    if (_writePos == -1) throw new CassandraConncectionIOException();

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

                    if (_writePos == -1) throw new CassandraConncectionIOException();

                    Buffer.BlockCopy(this._buffer, _readPos, buffer, offset, count);
                }

                _readPos += count;
            }
        }

        public void Skip(int count)
        {
            lock (_guard)
            {
                if (_writePos == -1) throw new CassandraConncectionIOException();

                _readPos += count;
            }
        }
    }
}
