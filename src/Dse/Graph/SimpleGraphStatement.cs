using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Cassandra;

namespace Dse.Graph
{
    public class SimpleGraphStatement : Statement
    {
        public override RoutingKey RoutingKey
        {
            get { return RoutingKey.Empty; }
        }
    }
}
