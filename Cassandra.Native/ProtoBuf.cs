using System;
using System.Collections.Generic;
using System.Text;
using System.IO;
using System.Threading;
using System.Diagnostics;

namespace Cassandra.Native
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
        Stream stream;
        bool ioError = false;
        IProtoBufComporessor compressor;
        public StreamProtoBuf(Stream stream, IProtoBufComporessor compressor)
        {
            this.stream = stream;
            this.compressor = compressor;
        }

        public void Write(byte[] buffer, int offset, int count)
        {
            if (ioError) throw new CassandraConncectionIOException();

            if (buffer == null)
                ioError = true;
            else
            {
                try
                {
                    lock(stream)
                        stream.Write(buffer, offset, count);
                }
                catch (IOException ex)
                {
                    Debug.WriteLine(ex.Message, "StreamProtoBuf.Write");
                    ioError = true;
                    throw new CassandraConncectionIOException(ex);
                }
            }
        }

        public void WriteByte(byte b)
        {
            if (ioError) throw new CassandraConncectionIOException();
            lock (stream)
                stream.WriteByte(b);
        }

        public void Read(byte[] buffer, int offset, int count)
        {
            if (count == 0) return;

            if (ioError) throw new CassandraConncectionIOException();

            int curOffset = offset;
            while (true)
            {
                try
                {
                    int redl;
                    lock (stream)
                    {
                        redl = stream.Read(buffer, curOffset, count - curOffset - offset);
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
                    ioError = true;
                    throw new CassandraConncectionIOException(ex);
                }
            }
        }

        byte[] trashBuf = new byte[10 * 1024];

        public void Skip(int count)
        {
            if (ioError) return;

            int curOffset = 0;
            while (true)
            {
                try
                {

                    int redl;
                    lock (stream)
                    {
                        redl = stream.Read(trashBuf, curOffset, count - curOffset);
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
                    ioError = true;
                    throw new CassandraConncectionIOException(ex);
                }
            }
        }

    }

    internal class BufferedProtoBuf : IProtoBuf
    {
        object guard = new object();
        byte[] buffer;
        int readPos;
        int writePos;
        IProtoBufComporessor compressor;
        byte[] decompressedBuffer;

        public BufferedProtoBuf(int bufferLength, IProtoBufComporessor compressor)
        {
            this.compressor = compressor;
            buffer = new byte[bufferLength];
            readPos = 0;
            writePos = 0;
        }

        public void Write(byte[] buffer, int offset, int count)
        {
            lock (guard)
            {
                if (writePos == -1) throw new CassandraConncectionIOException();

                if (buffer != null)
                {
                    Buffer.BlockCopy(buffer, offset, this.buffer, writePos, count);
                    writePos += count;
                }
                else
                    writePos = -1;
                Monitor.PulseAll(guard);
            }
        }

        public void WriteByte(byte b)
        {
            lock (guard)
            {
                if (writePos == -1) throw new CassandraConncectionIOException();

                buffer[writePos] = b;
                writePos++;
                Monitor.PulseAll(guard);
            }
        }

        public void Read(byte[] buffer, int offset, int count)
        {
            if (count == 0) return;
            lock (guard)
            {
                if (writePos == -1) throw new CassandraConncectionIOException();

                if (compressor != null)
                {
                    while (writePos != -1 && writePos < this.buffer.Length)
                        Monitor.Wait(guard);

                    if (writePos == -1) throw new CassandraConncectionIOException();

                    if (decompressedBuffer == null)
                        decompressedBuffer = compressor.Decompress(this.buffer);
                    
                    if (count > decompressedBuffer.Length - readPos)
                        throw new CassandraClientProtocolViolationException("Invalid decompression state");
                    
                    Buffer.BlockCopy(this.decompressedBuffer, readPos, buffer, offset, count);
                }
                else
                {
                    while (writePos != -1 && readPos + count > writePos)
                        Monitor.Wait(guard);

                    if (writePos == -1) throw new CassandraConncectionIOException();

                    Buffer.BlockCopy(this.buffer, readPos, buffer, offset, count);
                }

                readPos += count;
            }
        }

        public void Skip(int count)
        {
            lock (guard)
            {
                if (writePos == -1) throw new CassandraConncectionIOException();

                readPos += count;
            }
        }
    }
}
