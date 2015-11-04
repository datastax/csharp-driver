//
//      Copyright (C) 2012-2014 DataStax Inc.
//
//   Licensed under the Apache License, Version 2.0 (the "License");
//   you may not use this file except in compliance with the License.
//   You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
//   Unless required by applicable law or agreed to in writing, software
//   distributed under the License is distributed on an "AS IS" BASIS,
//   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//   See the License for the specific language governing permissions and
//   limitations under the License.
//

using System.IO;

namespace Cassandra.Requests
{
    internal interface IRequest
    {
        /// <summary>
        /// Gets the version of the protocol that the request is built for
        /// </summary>
        int ProtocolVersion { get; }
        /// <summary>
        /// Writes the frame for this request on the provided stream
        /// </summary>
        int WriteFrame(short streamId, MemoryStream stream);
    }
}
