using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace Cassandra
{
    /// <summary>
    /// Represents an CQL Request (BATCH, EXECUTE or QUERY)
    /// </summary>
    internal interface ICqlRequest
    {
        ConsistencyLevel Consistency { get; set; }
    }
}
