//
//  Copyright (C) 2017 DataStax, Inc.
//
//  Please see the license for details:
//  http://www.datastax.com/terms/datastax-dse-driver-license-terms
//

using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace Cassandra.Data.Linq
{
    /// <summary>
    /// Specifies that the field or property should be ignored by Linq
    /// </summary>
    [Obsolete("Linq attributes are deprecated, use mapping attributes defined in Cassandra.Mapping.Attributes instead.")]
    [AttributeUsage(AttributeTargets.Property | AttributeTargets.Field)]
    public class IgnoreAttribute : Attribute
    {
    }
}
