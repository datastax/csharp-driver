using System;
using System.Collections.Generic;
using System.Net;
namespace Cassandra
{
    /// <summary>
    ///  Informations and known state of a Cassandra cluster. <p> This is the main
    ///  entry point of the driver. A simple example of access to a Cassandra cluster
    ///  would be: 
    /// <pre> Cluster cluster = Cluster.Builder.AddContactPoint("192.168.0.1").Build(); 
    ///  Session session = Cluster.Connect("db1"); 
    ///  foreach (var row in session.execute("SELECT * FROM table1")) 
    ///    //do something ... </pre> 
    ///  </p><p> A cluster object maintains a
    ///  permanent connection to one of the cluster node that it uses solely to
    ///  maintain informations on the state and current topology of the cluster. Using
    ///  the connection, the driver will discover all the nodes composing the cluster
    ///  as well as new nodes joining the cluster.</p>
    /// </summary>
    public interface ICluster : IDisposable
    {
        /// <summary>
        ///  Returns all known hosts of this cluster.
        /// </summary>
        ICollection<Host> AllHosts();
        /// <summary>
        ///  Creates a new session on this cluster.
        /// </summary>
        /// <returns>a new session on this cluster set to no keyspace.</returns>
        ISession Connect();
        /// <summary>
        ///  Creates a new session on this cluster and sets a keyspace to use.
        /// </summary>
        /// <param name="keyspace"> The name of the keyspace to use for the created <c>ISession</c>. </param>
        /// <returns>a new session on this cluster set to keyspace: 
        ///  <c>keyspaceName</c>. </returns>
        ISession Connect(string keyspace);
        /// <summary>
        /// Get the host instance for a given Ip address.
        /// </summary>
        /// <param name="address">Ip address of the host</param>
        /// <returns>The host or null if not found</returns>
        Host GetHost(IPAddress address);
        /// <summary>
        /// Gets a collection of replicas for a given partitionKey
        /// </summary>
        /// <param name="partitionKey">Byte array representing the partition key</param>
        /// <returns></returns>
        ICollection<IPAddress> GetReplicas(byte[] partitionKey);
        /// <summary>
        ///  Shutdown this cluster instance. This closes all connections from all the
        ///  sessions of this <c>* Cluster</c> instance and reclaim all resources
        ///  used by it. <p> This method has no effect if the cluster was already shutdown.</p>
        /// </summary>
        void Shutdown(int timeoutMs = System.Threading.Timeout.Infinite);
    }
}
