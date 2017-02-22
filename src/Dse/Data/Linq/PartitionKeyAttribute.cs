//
//  Copyright (C) 2017 DataStax, Inc.
//
//  Please see the license for details:
//  http://www.datastax.com/terms/datastax-dse-driver-license-terms
//

using System;

namespace Dse.Data.Linq
{
    /// <summary>
    /// Indicates that the property or field is part of the Partition Key
    /// </summary>
    [Obsolete("Linq attributes are deprecated, use mapping attributes defined in Dse.Mapping.Attributes instead.")]
    [AttributeUsage(AttributeTargets.Property | AttributeTargets.Field, Inherited = true, AllowMultiple = true)]
    public class PartitionKeyAttribute : Attribute
    {
        public int Index { get; set; }

        public PartitionKeyAttribute(int index = 0)
        {
            Index = index;
        }
    }
}