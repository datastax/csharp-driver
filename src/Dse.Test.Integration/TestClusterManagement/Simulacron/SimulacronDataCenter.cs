//
//  Copyright (C) DataStax, Inc.
//
//  Please see the license for details:
//  http://www.datastax.com/terms/datastax-dse-driver-license-terms
//

using System.Collections.Generic;

namespace Dse.Test.Integration.TestClusterManagement.Simulacron
{
    public class SimulacronDataCenter : SimulacronBase
    {
        public List<SimulacronNode> Nodes { get; set; }
        public SimulacronDataCenter(string id): base(id)
        {
        }
    }
}