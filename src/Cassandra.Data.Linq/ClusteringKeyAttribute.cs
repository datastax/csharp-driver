using System;

namespace Cassandra.Data.Linq
{
    [AttributeUsage(AttributeTargets.Property | AttributeTargets.Field, Inherited = true, AllowMultiple = true)]
    public class ClusteringKeyAttribute : Attribute
    {
        public string ClusteringOrder = null;
        public int Index = -1;
        public string Name;

        public ClusteringKeyAttribute(int index)
        {
            Index = index;
        }

        /// <summary>
        /// Sets the clustering key and optionally a clustering order for it.
        /// </summary>
        /// <param name="index"></param>
        /// <param name="order">Use "DESC" for descending order and "ASC" for ascending order.</param>
        public ClusteringKeyAttribute(int index, string order)
        {
            Index = index;

            if (order == "DESC" || order == "ASC")
                ClusteringOrder = order;
            else
                throw new ArgumentException("Possible arguments are: \"DESC\" - for descending order and \"ASC\" - for ascending order.");
        }
    }
}