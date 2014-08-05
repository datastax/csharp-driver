namespace CqlPoco
{
    /// <summary>
    /// The contract for synchronous write operations.
    /// </summary>
    public interface ICqlWriteClient
    {
        /// <summary>
        /// Inserts the specified POCO in Cassandra.
        /// </summary>
        void Insert<T>(T poco);

        /// <summary>
        /// Updates the POCO specified in Cassandra.
        /// </summary>
        void Update<T>(T poco);

        /// <summary>
        /// Updates the table for the POCO type specified (T) using the CQL string and bind variable values specified.  Prepends "UPDATE tablename " to the CQL
        /// string you specify, getting the tablename appropriately from the POCO Type T.
        /// </summary>
        /// <typeparam name="T">The POCO Type being updated.</typeparam>
        void Update<T>(string cql, params object[] args);

        /// <summary>
        /// Deletes the specified POCO from Cassandra.
        /// </summary>
        void Delete<T>(T poco);

        /// <summary>
        /// Deletes from the table for the POCO type specified (T) using the CQL string and bind variable values specified.  Prepends "DELETE FROM tablname " to
        /// the CQL string you specify, getting the tablename appropriately from the POCO Type T.
        /// </summary>
        void Delete<T>(string cql, params object[] args);

        /// <summary>
        /// Executes an arbitrary CQL string with the bind variable values specified.
        /// </summary>
        void Execute(string cql, params object[] args);
    }
}