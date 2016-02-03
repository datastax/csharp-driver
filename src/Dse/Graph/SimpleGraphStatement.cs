using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Cassandra;

namespace Dse.Graph
{
    public class SimpleGraphStatement : Statement
    {
        private RoutingKey _routingKey;

        public override RoutingKey RoutingKey
        {
            get { return _routingKey; }
        }
    }
}
