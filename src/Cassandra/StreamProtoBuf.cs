//
//      Copyright (C) 2012 DataStax Inc.
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

using System.IO;

namespace Cassandra
{
    internal class StreamProtoBuf : IProtoBuf
    {
        private readonly Logger _logger = new Logger(typeof (StreamProtoBuf));
        private readonly Stream _stream;
        private readonly byte[] _trashBuf = new byte[10*1024];
        private IProtoBufComporessor _compressor;
        private bool _ioError;

        public StreamProtoBuf(Stream stream, IProtoBufComporessor compressor)
        {
            _stream = stream;
            _compressor = compressor;
        }

        public void Write(byte[] buffer, int offset, int count)
        {
            if (buffer == null)
                _ioError = true;
            else
            {
                if (_ioError) throw new CassandraConnectionIOException();

                try
                {
                    _stream.Write(buffer, offset, count);
                }
                catch (IOException ex)
                {
                    _logger.Error("Writing to StreamProtoBuf failed: ", ex);
                    _ioError = true;
                    throw new CassandraConnectionIOException(ex);
                }
            }
        }

        public void WriteByte(byte b)
        {
            if (_ioError) throw new CassandraConnectionIOException();
            _stream.WriteByte(b);
        }

        public void Read(byte[] buffer, int offset, int count)
        {
            if (count == 0) return;

            if (_ioError) throw new CassandraConnectionIOException();

            int curOffset = offset;
            while (true)
            {
                try
                {
                    int redl = _stream.Read(buffer, curOffset, count - curOffset - offset);
                    if (redl == 0)
                    {
                        throw new CassandraConnectionIOException();
                    }
                    if (redl == count - curOffset - offset)
                    {
                        return;
                    }
                    curOffset += redl;
                }
                catch (IOException ex)
                {
                    _logger.Error("Reading from StreamProtoBuf failed: ", ex);
                    _ioError = true;
                    throw new CassandraConnectionIOException(ex);
                }
            }
        }

        public void Skip(int count)
        {
            if (_ioError) return;

            int curOffset = 0;
            while (true)
            {
                try
                {
                    int redl = _stream.Read(_trashBuf, curOffset, count - curOffset);
                    if (redl == 0)
                    {
                        throw new CassandraConnectionIOException();
                    }
                    if (redl == count - curOffset)
                    {
                        return;
                    }
                    curOffset += redl;
                }
                catch (IOException ex)
                {
                    _logger.Error("Skipping in StreamProtoBuf failed: ", ex);
                    _ioError = true;
                    throw new CassandraConnectionIOException(ex);
                }
            }
        }
    }
}