using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Cassandra
{
    /// <summary>
    /// Creates a task for the response with null result
    /// </summary>
    internal class ResponseSource : ResponseSource<object>
    {

    }

    /// <summary>
    /// Creates a task for the response
    /// </summary>
    /// <typeparam name="TOutput">Type for the Task Result</typeparam>
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
            _tcs.TrySetResult((TOutput) (object) response);
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
