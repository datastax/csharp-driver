﻿using System;
using System.Collections.Generic;
using System.Linq;
using System.Numerics;
using System.Text;

namespace Cassandra.Tests.Mapping.Pocos
{
    public class VarintPoco
    {
        public Guid Id { get; set; }

        public string Name { get; set; }

        public BigInteger VarintValue { get; set; }
    }
}
