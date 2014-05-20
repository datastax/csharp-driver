using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Cassandra
{
    /// <summary>
    /// Represents the state of the ongoing operation for the Connection
    /// </summary>
    internal class OperationState
    {
        public int BytesRead { get; set; }

        /// <summary>
        /// Returns true if there are enough data to parse body
        /// </summary>
        public bool IsBodyComplete
        {
            get 
            {
                if (this.Header == null)
                {
                    throw new NullReferenceException("The frame header can not be null");
                }
                if (this.ReadBuffer == null)
                {
                    return false;
                }
                return this.ReadBuffer.Length >= this.Header.TotalFrameLength;
            }
        }

        /// <summary>
        /// Total frame length: header + body
        /// </summary>
        public int FrameLength { get; set; }

        /// <summary>
        /// 8 byte header of the frame
        /// </summary>
        public FrameHeader Header { get; set; }

        /// <summary>
        /// Read Buffer
        /// </summary>
        public byte[] ReadBuffer { get; set; }

        public IRequest Request { get; set; }

        public TaskCompletionSource<AbstractResponse> TaskCompletionSource { get; set; }

        public void AddBuffer(byte[] value)
        {
            if (this.ReadBuffer == null)
            {
                this.ReadBuffer = value;
            }
            else
            {
                this.ReadBuffer = Utils.JoinBuffers(this.ReadBuffer, value);
            }
        }
    }
}
