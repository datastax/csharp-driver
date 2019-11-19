// 
//       Copyright (C) DataStax Inc.
// 
//    Licensed under the Apache License, Version 2.0 (the "License");
//    you may not use this file except in compliance with the License.
//    You may obtain a copy of the License at
// 
//       http://www.apache.org/licenses/LICENSE-2.0
// 
//    Unless required by applicable law or agreed to in writing, software
//    distributed under the License is distributed on an "AS IS" BASIS,
//    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//    See the License for the specific language governing permissions and
//    limitations under the License.

using System.Collections.Generic;
using System.Net;

namespace Cassandra.Tests.TestHelpers
{
    public class FakeInitializer : IInitializer
    {
        private readonly Configuration _config;
        private readonly IEnumerable<IPEndPoint> _endpts;

        public FakeInitializer(Configuration config)
        {
            _config = config;
        }
        
        public FakeInitializer(Configuration config, IEnumerable<IPEndPoint> endpts)
        {
            _config = config;
            _endpts = endpts;
        }

        public ICollection<IPEndPoint> ContactPoints => new List<IPEndPoint>(_endpts);

        public Configuration GetConfiguration()
        {
            return _config;
        }
    }
}