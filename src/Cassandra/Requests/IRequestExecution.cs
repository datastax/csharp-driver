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

namespace Cassandra.Requests
{
    internal interface IRequestExecution
    {
        void Cancel();

        /// <summary>
        /// Starts a new execution using the current request. Note that an I/O task is scheduled here in a fire and forget manner.
        /// <para/>
        /// In some scenarios, some exceptions are thrown before the scheduling of any I/O task in order to fail fast.
        /// </summary>
        /// <param name="currentHostRetry">Whether this is a retry on the last queried host.
        /// Usually this is mapped from <see cref="RetryDecision.UseCurrentHost"/></param>
        /// <returns>Host chosen to which a connection will be obtained first.
        /// The actual host that will be queried might be different if a connection is not successfully obtained.
        /// In this scenario, the next host will be chosen according to the query plan.</returns>
        Host Start(bool currentHostRetry);
    }
}