namespace Cassandra
{
    /// <summary>
    /// Represents a QUERY or EXECUTE request that can be included in a batch
    /// </summary>
    internal interface IQueryRequest : IRequest
    {
        /// <summary>
        /// The paging state for the request
        /// </summary>
        byte[] PagingState { get; set; }
        /// <summary>
        /// Method used by the batch to build each individual request
        /// </summary>
        void WriteToBatch(byte protocolVersion, BEBinaryWriter writer);
    }
}