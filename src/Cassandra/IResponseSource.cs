using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace Cassandra
{
    /// <summary>
    /// Represents a Task source that handles the setting of results, response errors and exceptions.
    /// </summary>
    internal interface IResponseSource
    {
        void SetResponse(AbstractResponse response);
        void SetException(Exception ex);
    }
}
