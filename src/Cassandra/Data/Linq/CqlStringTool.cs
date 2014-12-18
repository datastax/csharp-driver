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

using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace Cassandra.Data.Linq
{
    internal class CqlStringTool
    {
        private readonly List<object> srcvalues = new List<object>();

        public string FillWithEncoded(string pure)
        {
            if (srcvalues.Count == 0)
                return pure;

            var sb = new StringBuilder();
            string[] parts = pure.Split('\0');

            for (int i = 0; i < parts.Length - 1; i += 2)
            {
                sb.Append(parts[i]);
                int idx = int.Parse(parts[i + 1]);
                var cqlValue = srcvalues[idx] != null
                    ? srcvalues[idx].Encode()
                    : "null";
                sb.Append(cqlValue);
            }
            sb.Append(parts.Last());
            return sb.ToString();
        }

        public string FillWithValues(string pure, out object[] values)
        {
            if (srcvalues.Count == 0)
            {
                values = null;
                return pure;
            }

            var sb = new StringBuilder();
            var objs = new List<object>();
            string[] parts = pure.Split('\0');

            for (int i = 0; i < parts.Length - 1; i += 2)
            {
                sb.Append(parts[i]);
                int idx = int.Parse(parts[i + 1]);
                objs.Add(srcvalues[idx]);
                sb.Append(" ? ");
            }
            sb.Append(parts.Last());
            values = objs.ToArray();
            return sb.ToString();
        }

        public string AddValue(object val)
        {
            srcvalues.Add(val);
            return "\0" + (srcvalues.Count - 1) + "\0";
        }
    }
}