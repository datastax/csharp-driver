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

﻿namespace Cassandra
{
    /// <summary>
    /// The type of batch to use
    /// </summary>
    public enum BatchType
    {
        /// <summary>
        /// A logged batch: Cassandra will first write the batch to its distributed batch log to ensure the atomicity of the batch.
        /// </summary>
        Logged = 0,
        /// <summary>
        /// An unlogged batch: The batch will not be written to the batch log and atomicity of the batch is NOT guaranteed.
        /// </summary>
        Unlogged = 1,
        /// <summary>
        /// A counter batch
        /// </summary>
        Counter = 2
    }
}
