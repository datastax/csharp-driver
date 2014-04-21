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

using System.Collections.Generic;

namespace Cassandra
{
    internal class OutputOptions : IOutput, IWaitableForDispose
    {
        private readonly Dictionary<string, string[]> _options;

        public IDictionary<string, string[]> Options
        {
            get { return _options; }
        }

        internal OutputOptions(BEBinaryReader reader)
        {
            _options = new Dictionary<string, string[]>();
            int n = reader.ReadUInt16();
            for (int i = 0; i < n; i++)
            {
                string k = reader.ReadString();
                string[] v = reader.ReadStringList().ToArray();
                _options.Add(k, v);
            }
        }

        public void Dispose()
        {
        }

        public void WaitForDispose()
        {
        }

        public System.Guid? TraceId
        {
            get;
            internal set;
        }
    }
}