using System;
using System.Collections.Generic;
using System.Linq;

namespace Cassandra.Data.Linq
{
    internal class CqlEqualityComparer<TEntity> : IEqualityComparer<TEntity>
    {
        public static CqlEqualityComparer<TEntity> Default = new CqlEqualityComparer<TEntity>();

        public bool Equals(TEntity x, TEntity y)
        {
            var props = typeof(TEntity).GetPropertiesOrFields();
            foreach (var prop in props)
            {
                var pk = prop.GetCustomAttributes(typeof(PartitionKeyAttribute), true).FirstOrDefault() as PartitionKeyAttribute;
                if (pk != null)
                {
                    if (prop.GetValueFromPropertyOrField(x) == null)
                        throw new InvalidOperationException("Partition Key is not set");
                    if (prop.GetValueFromPropertyOrField(y) == null)
                        throw new InvalidOperationException("Partition Key is not set");
                    if (!prop.GetValueFromPropertyOrField(x).Equals(prop.GetValueFromPropertyOrField(y)))
                        return false;
                }
                else
                {
                    var rk = prop.GetCustomAttributes(typeof(ClusteringKeyAttribute), true).FirstOrDefault() as ClusteringKeyAttribute;
                    if (rk != null)
                    {
                        if (prop.GetValueFromPropertyOrField(x) == null)
                            throw new InvalidOperationException("Clustering Key is not set");
                        if (prop.GetValueFromPropertyOrField(y) == null)
                            throw new InvalidOperationException("Clustering Key is not set");
                        if (!prop.GetValueFromPropertyOrField(x).Equals(prop.GetValueFromPropertyOrField(y)))
                            return false;
                    }
                }
            }
            return true;
        }

        public int GetHashCode(TEntity obj)
        {
            int hashCode = 0;
            var props = typeof(TEntity).GetPropertiesOrFields();
            foreach (var prop in props)
            {
                var pk = prop.GetCustomAttributes(typeof(PartitionKeyAttribute), true).FirstOrDefault() as PartitionKeyAttribute;
                if (pk != null)
                {
                    if (prop.GetValueFromPropertyOrField(obj) == null)
                        throw new InvalidOperationException("Partition Key is not set");
                    hashCode ^= prop.GetValueFromPropertyOrField(obj).GetHashCode();
                }
                else
                {
                    var rk = prop.GetCustomAttributes(typeof(ClusteringKeyAttribute), true).FirstOrDefault() as ClusteringKeyAttribute;
                    if (rk != null)
                    {
                        if (prop.GetValueFromPropertyOrField(obj) == null)
                            throw new InvalidOperationException("Clustering Key is not set");
                        hashCode ^= prop.GetValueFromPropertyOrField(obj).GetHashCode();
                    }
                }
            }
            return hashCode;
        }
    }
}