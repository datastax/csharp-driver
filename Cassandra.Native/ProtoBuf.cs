using System;
using System.Collections.Generic;
using System.Text;
using System.IO;
using System.Threading;
using System.Diagnostics;

namespace Cassandra.Native
{

    internal class IOCassandraException : Exception
    {
        public IOCassandraException(Exception innerException = null)
            : base("cassandra io exception",innerException)
        {
        }
    }

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
            if (ioError) throw new IOCassandraException();

            if (buffer == null)
                ioError = true;
            else
            {
                try
                {
                    stream.Write(buffer, offset, count);
                }
                catch (IOException ex)
                {
                    Debug.WriteLine(ex.Message, "StreamProtoBuf.Write");
                    ioError = true;
                    throw new IOCassandraException(ex);
                }
            }
        }

        public void WriteByte(byte b)
        {
            if (ioError) throw new IOCassandraException();
            stream.WriteByte(b);
        }

        public void Read(byte[] buffer, int offset, int count)
        {
            if (ioError) throw new IOCassandraException();

            int curOffset = offset;
            while (true)
            {
                try
                {
                    var redl = stream.Read(buffer, curOffset, count - curOffset - offset);
                    if (redl == 0)
                    {
                        throw new IOCassandraException();
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
                    throw new IOCassandraException(ex);
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

                    var redl = stream.Read(trashBuf, curOffset, count - curOffset);
                    if (redl == 0)
                    {
                        throw new IOCassandraException();
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
                    throw new IOCassandraException(ex);
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
                if (writePos == -1) throw new IOCassandraException();

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
                if (writePos == -1) throw new IOCassandraException();

                buffer[writePos] = b;
                writePos++;
                Monitor.PulseAll(guard);
            }
        }

        public void Read(byte[] buffer, int offset, int count)
        {
            lock (guard)
            {
                if (writePos == -1) throw new IOCassandraException();

                if (compressor != null)
                {
                    while (writePos != -1 && writePos < this.buffer.Length)
                        Monitor.Wait(guard);

                    if (writePos == -1) throw new IOCassandraException();

                    if (decompressedBuffer == null)
                        decompressedBuffer = compressor.Decompress(this.buffer);
                    
                    if (count > decompressedBuffer.Length - readPos)
                        throw new InvalidOperationException();

                    Buffer.BlockCopy(this.decompressedBuffer, readPos, buffer, offset, count);
                }
                else
                {
                    while (writePos != -1 && readPos + count > writePos)
                        Monitor.Wait(guard);

                    if (writePos == -1) throw new IOCassandraException();

                    Buffer.BlockCopy(this.buffer, readPos, buffer, offset, count);
                }

                readPos += count;
            }
        }

        public void Skip(int count)
        {
            lock (guard)
            {
                if (writePos == -1) throw new IOCassandraException();

                readPos += count;
            }
        }
    }
}
