//
//  Copyright (C) DataStax, Inc.
//
//  Please see the license for details:
//  http://www.datastax.com/terms/datastax-dse-driver-license-terms
//

using System;
using System.Numerics;

namespace Dse.Test.Unit.Mapping.Pocos
{
    public class VarintPoco
    {
        public Guid Id { get; set; }

        public string Name { get; set; }

        public BigInteger VarintValue { get; set; }
    }
}
