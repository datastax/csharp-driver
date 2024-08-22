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

using System.Collections.Concurrent;

namespace Cassandra
{
    public class RequestTrackingInfo
    {
        private readonly IStatement _statement;

        public RequestTrackingInfo(IStatement statement)
        {
            this._statement = statement;
            this.Items = new ConcurrentDictionary<string, object>();
        }

        public ConcurrentDictionary<string, object> Items { get; }

        public IStatement Statement => _statement;
    }
}
