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
    ///  A server timeout during a read query.
    /// </summary>
    public class ReadTimeoutException : QueryTimeoutException
    {
        public bool WasDataRetrieved { get; private set; }

        public ReadTimeoutException(ConsistencyLevel consistency, int received, int required, bool dataPresent) :
            base("Server timeout during read query at consistency" +
                 $" {consistency} ({FormatDetails(received, required, dataPresent)})",
                 consistency,
                 received,
                 required)
        {
            WasDataRetrieved = dataPresent;
        }

        private static string FormatDetails(int received, int required, bool dataPresent)
        {
            if (received < required)
            {
                return $"{received} replica(s) responded over {required} required";
            }

            if (!dataPresent)
            {
                return "the replica queried for data didn't respond";
            }
            return "timeout while waiting for repair of inconsistent replica";
        }
    }
}
