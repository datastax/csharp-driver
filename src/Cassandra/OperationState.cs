using System;
using System.Collections.Generic;
using System.IO;
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
        /// <summary>
        /// Gets a readable stream representing the body
        /// </summary>
        public Stream BodyStream { get; private set; }

        /// <summary>
        /// Returns true if there are enough data to parse body
        /// </summary>
        public bool IsBodyComplete
        {
            get 
            {
                if (BodyStream is MemoryStream)
                {
                    return true;
                }
                if (BodyStream is ListBackedStream)
                {
                    return ((ListBackedStream)BodyStream).TotalLength == Header.BodyLength;
                }
                return false;
            }
        }

        /// <summary>
        /// 8 byte header of the frame
        /// </summary>
        public FrameHeader Header { get; set; }

        public IRequest Request { get; set; }

        public IResponseSource ResponseSource { get; set; }

        /// <summary>
        /// Appends to the body stream
        /// </summary>
        /// <returns>The total amount of bytes added</returns>
        public int AppendBody(byte[] value, int offset, int count)
        {
            if (Header == null)
            {
                throw new DriverInternalError("To add a response body you must specify the header");
            }
            if (Header.BodyLength <= count)
            {
                //There is no need to copy the buffer: Use the inner buffer
                BodyStream = new MemoryStream(value, offset, this.Header.BodyLength, false, false);
                return this.Header.BodyLength;
            }
            if (BodyStream == null)
            {
                BodyStream = new ListBackedStream(Header.BodyLength);
            }
            if (BodyStream.Position + count > Header.BodyLength)
            {
                count = Header.BodyLength - (int) BodyStream.Position;
            }
            BodyStream.Write(value, offset, count);
            return count;
        }
    }

    /// <summary>
    /// Represents a Task source that handles the setting of results, response errors and exceptions.
    /// </summary>
    internal interface IResponseSource
    {
        void SetResponse(AbstractResponse response);
        void SetException(Exception ex);
    }

    /// <summary>
    /// Creates a task for the response with null result
    /// </summary>
    internal class ResponseSource : ResponseSource<object>
    {

    }

    /// <summary>
    /// Creates a task for the response
    /// </summary>
    /// <typeparam name="TResponse">Type for the Task Result</typeparam>
    internal class ResponseSource<TOutput> : IResponseSource
    {
        private TaskCompletionSource<TOutput> _tcs;

        public ResponseSource()
            : this(new TaskCompletionSource<TOutput>())
        {

        }

        /// <param name="tcs">The Task completion source used to create the task</param>
        public ResponseSource(TaskCompletionSource<TOutput> tcs)
        {
            _tcs = tcs;
        }

        /// <summary>
        /// Gets the task that will be completed once the response is received
        /// </summary>
        public Task<TOutput> Task
        {
            get
            {
                return _tcs.Task;
            }
        }

        public void SetException(Exception ex)
        {
            _tcs.TrySetException(ex);
        }

        public void SetResponse(AbstractResponse response)
        {
            if (response is ErrorResponse)
            {
                SetErrorResponse((ErrorResponse)response);
                return;
            }
            object value = null;
            //Determine if its a RowSet
            if (response is ResultResponse && typeof(TOutput) == typeof(RowSet))
            {
                value = ((ResultResponse)response).ToRowSet();
            }
            _tcs.SetResult((TOutput)value);
        }

        /// <summary>
        /// Extracts the error from the response and sets the underlying task as faulted.
        /// </summary>
        private void SetErrorResponse(ErrorResponse response)
        {
            _tcs.SetException(response.Output.CreateException());
        }
    }
}
