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

namespace Dse.Mapping.Attributes
{
    /// <summary>
    /// Indicates that the property or field represents a column which value is frozen.
    /// Only valid for maps and lists.
    /// </summary>
    [AttributeUsage(AttributeTargets.Property | AttributeTargets.Field)]
    public class FrozenValueAttribute : Attribute
    {

    }
}
