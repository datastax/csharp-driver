namespace Cassandra
{
    /// <summary>
    /// The type of batch to use
    /// </summary>
    public enum BatchType
    {
        /// <summary>
        /// A logged batch: Cassandra will first the batch to its distributed batch log to ensure the atomicity of the batch.
        /// </summary>
        Logged = 0,
        /// <summary>
        /// A logged batch: Cassandra will first the batch to its distributed batch log to ensure the atomicity of the batch.
        /// </summary>
        Unlogged = 1,
        /// <summary>
        /// A counter batch
        /// </summary>
        Counter = 2
    }
}