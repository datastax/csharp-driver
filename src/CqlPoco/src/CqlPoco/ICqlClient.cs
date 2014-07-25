using System.Threading.Tasks;

namespace CqlPoco
{
    /// <summary>
    /// The client for creating, updating, deleting, and reading POCOs from a Cassandra cluster.
    /// </summary>
    public interface ICqlClient : ICqlQueryClient
    {
        /// <summary>
        /// Inserts the specified POCO in Cassandra.
        /// </summary>
        Task Insert<T>(T poco);

        /// <summary>
        /// Updates the POCO specified in Cassandra.
        /// </summary>
        Task Update<T>(T poco);

        /// <summary>
        /// Deletes the specified POCO from Cassandra.
        /// </summary>
        Task Delete<T>(T poco);
    }
}
