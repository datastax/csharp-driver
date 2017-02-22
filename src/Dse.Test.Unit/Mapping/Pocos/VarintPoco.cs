//
//  Copyright (C) 2017 DataStax, Inc.
//
//  Please see the license for details:
//  http://www.datastax.com/terms/datastax-dse-driver-license-terms
//

using System;
using System.Collections.Generic;
using System.Linq;
using System.Numerics;
using System.Text;

namespace Dse.Test.Unit.Mapping.Pocos
{
    public class VarintPoco
    {
        public Guid Id { get; set; }

        public string Name { get; set; }

        public BigInteger VarintValue { get; set; }
    }
}
