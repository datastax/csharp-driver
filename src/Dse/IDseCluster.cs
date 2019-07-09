//
//  Copyright (C) DataStax, Inc.
//
//  Please see the license for details:
//  http://www.datastax.com/terms/datastax-dse-driver-license-terms
//

using System.Threading.Tasks;

namespace Dse
{
    /// <summary>
    /// Represents a DSE cluster client that contains information and known state of a DSE cluster.
    /// </summary>
    public interface IDseCluster : ICluster
    {
        /// <summary>
        /// Gets the DSE cluster client configuration.
        /// </summary>
        new DseConfiguration Configuration { get; }

        /// <summary>
        /// Creates a new DSE session on this cluster and initializes it.
        /// </summary>
        /// <returns>A new <see cref="IDseSession"/> instance.</returns>
        new IDseSession Connect();

        /// <summary>
        /// Creates a new DSE session on this cluster, initializes it and sets the keyspace to the provided one.
        /// </summary>
        /// <param name="keyspace">The keyspace to connect to</param>
        /// <returns>A new <see cref="IDseSession"/> instance.</returns>
        new IDseSession Connect(string keyspace);

        /// <summary>
        /// Creates a new DSE session on this cluster.
        /// </summary>
        new Task<IDseSession> ConnectAsync();

        /// <summary>
        /// Creates a new DSE session on this cluster and using a keyspace an existing keyspace.
        /// </summary>
        /// <param name="keyspace">Case-sensitive keyspace name to use</param>
        new Task<IDseSession> ConnectAsync(string keyspace);
    }
}