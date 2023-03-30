//
//      Copyright (C) DataStax Inc.
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

namespace Cassandra
{
    /// <summary>
    /// Specifies the different graph protocol versions that are supported by the driver
    /// </summary>
    public enum GraphProtocol
    {
        /// <summary>
        /// GraphSON v1.
        /// </summary>
        GraphSON1 = 1,

        /// <summary>
        /// GraphSON v2.
        /// </summary>
        GraphSON2 = 2,

        /// <summary>
        /// GraphSON v3.
        /// </summary>
        GraphSON3 = 3
    }
}