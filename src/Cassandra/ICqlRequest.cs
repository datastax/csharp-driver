using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace Cassandra
{
    internal interface ICqlRequest
    {
        ConsistencyLevel Consistency { get; set; }
    }
}
