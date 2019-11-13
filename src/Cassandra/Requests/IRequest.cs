//
//  Copyright (C) DataStax, Inc.
//
//  Please see the license for details:
//  http://www.datastax.com/terms/datastax-dse-driver-license-terms
//

using System.IO;
using Cassandra.Serialization;

namespace Cassandra.Requests
{
    internal interface IRequest
    {
        /// <summary>
        /// Writes the frame for this request on the provided stream
        /// </summary>
        int WriteFrame(short streamId, MemoryStream stream, Serializer serializer);
    }
}
