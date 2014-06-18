using System;

namespace Cassandra.Data.Linq
{
    /// <summary>
    /// The ALLOW FILTERING option allows to explicitly allow queries that require filtering. 
    /// Please note that a query using ALLOW FILTERING may thus have unpredictable performance (for the definition above), i.e. even a query that selects a handful of records may exhibit performance that depends on the total amount of data stored in the cluster.
    /// </summary>
    [AttributeUsage(AttributeTargets.Class | AttributeTargets.Struct, Inherited = false, AllowMultiple = false)]
    public class AllowFilteringAttribute : Attribute
    {
    }
}