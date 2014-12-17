﻿using System;

namespace Cassandra.Mapping.Attributes
{
    /// <summary>
    /// Tells the mapper to ignore mapping this property.
    /// </summary>
    [AttributeUsage(AttributeTargets.Property | AttributeTargets.Field)]
    public class IgnoreAttribute : Attribute
    {
    }
}