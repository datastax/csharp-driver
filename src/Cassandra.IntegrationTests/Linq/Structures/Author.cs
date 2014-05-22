//
//      Copyright (C) 2012 DataStax Inc.
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

using System;
using System.Collections.Generic;
using Cassandra.Data.Linq;
using System.Diagnostics;

namespace Cassandra.IntegrationTests.Linq.Structures
{
    public class Author
    {
        [PartitionKey] public string author_id;

        public List<string> followers;

        public void displayFollowers()
        {
            if (followers != null)
                foreach (string follower in followers)
                    Trace.TraceInformation(follower + "  ");
            else
                Trace.TraceInformation("Nobody!");
        }
    }
}