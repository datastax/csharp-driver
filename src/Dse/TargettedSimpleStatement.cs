//
//  Copyright (C) DataStax, Inc.
//
//  Please see the license for details:
//  http://www.datastax.com/terms/datastax-dse-driver-license-terms
//
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Dse;

namespace Dse
{
    internal class TargettedSimpleStatement : SimpleStatement
    {
        /// <summary>
        /// The preferred host to be used by the load balancing policy.
        /// </summary>
        public Host PreferredHost { get; set; }

        public TargettedSimpleStatement(string query, params object[] values)
            : base(query, values)
        {

        }

        public TargettedSimpleStatement(string query)
            : base(query)
        {

        }
    }
}
