//
//  Copyright (C) 2017 DataStax, Inc.
//
//  Please see the license for details:
//  http://www.datastax.com/terms/datastax-dse-driver-license-terms
//

using System;

namespace Cassandra.Data.Linq
{
    /// <summary>
    /// Indicates that the property or field is a counter column
    /// </summary>
    [Obsolete("Linq attributes are deprecated, use mapping attributes defined in Cassandra.Mapping.Attributes instead.")]
    [AttributeUsage(AttributeTargets.Property | AttributeTargets.Field, Inherited = true, AllowMultiple = true)]
    public class CounterAttribute : Attribute
    {
    }
}