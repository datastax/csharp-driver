//
//  Copyright (C) DataStax, Inc.
//
//  Please see the license for details:
//  http://www.datastax.com/terms/datastax-dse-driver-license-terms
//

namespace Dse
{
    /// <summary>
    /// Describes the type of key.
    /// <para>
    /// This enum is going to be deprecated in future releases, use
    /// <see cref="DataCollectionMetadata.PartitionKeys"/>, <see cref="DataCollectionMetadata.ClusteringKeys"/>
    /// and <see cref="TableMetadata.Indexes"/> for a more accurate representation of a table or view keys and
    /// indexes.
    /// </para>
    /// </summary>
    public enum KeyType
    {
        None = 0,
        Partition = 1,
        Clustering = 2,
        SecondaryIndex = 3
    }
}