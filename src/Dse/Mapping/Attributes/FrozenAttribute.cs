//
//  Copyright (C) DataStax, Inc.
//
//  Please see the license for details:
//  http://www.datastax.com/terms/datastax-dse-driver-license-terms
//

using System;

namespace Dse.Mapping.Attributes
{
    /// <summary>
    /// Indicates that the property or field is Frozen.
    /// Only valid for collections, tuples, and user-defined types.
    /// </summary>
    [AttributeUsage(AttributeTargets.Property | AttributeTargets.Field)]
    public class FrozenAttribute : Attribute
    {

    }
}
