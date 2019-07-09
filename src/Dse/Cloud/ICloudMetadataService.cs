// 
//       Copyright (C) DataStax, Inc.
// 
//     Please see the license for details:
//     http://www.datastax.com/terms/datastax-dse-driver-license-terms
// 

using System.Threading.Tasks;

namespace Dse.Cloud
{
    /// <summary>
    /// Client to interact with the Cloud Metadata Service.
    /// </summary>
    internal interface ICloudMetadataService
    {
        /// <summary>
        /// Retrieve the cloud cluster's metadata from the cloud metadata service.
        /// </summary>
        /// <param name="url">Metadata endpoint</param>
        /// <param name="certFile">Client certificate.</param>
        Task<CloudMetadataResult> GetClusterMetadataAsync(string url, string certFile);
    }
}