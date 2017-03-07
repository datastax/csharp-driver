//
//  Copyright (C) 2016 DataStax, Inc.
//
//  Please see the license for details:
//  http://www.datastax.com/terms/datastax-dse-driver-license-terms
//
using System;
using Dse;

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
    }
}
