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

namespace Cassandra
{
    public class RoutingKey
    {
        public static RoutingKey Empty = new RoutingKey();
        public byte[] RawRoutingKey = null;

        public static RoutingKey Compose(params RoutingKey[] components)
        {
            if (components.Length == 0)
                throw new ArgumentOutOfRangeException();

            if (components.Length == 1)
                return components[0];

            int totalLength = 0;
            foreach (RoutingKey bb in components)
                totalLength += 2 + bb.RawRoutingKey.Length + 1;

            var res = new byte[totalLength];
            int idx = 0;
            foreach (RoutingKey bb in components)
            {
                PutShortLength(res, idx, bb.RawRoutingKey.Length);
                idx += 2;
                Buffer.BlockCopy(bb.RawRoutingKey, 0, res, idx, bb.RawRoutingKey.Length);
                idx += bb.RawRoutingKey.Length;
                res[idx] = 0;
                idx++;
            }
            return new RoutingKey {RawRoutingKey = res};
        }

        private static void PutShortLength(byte[] bb, int idx, int length)
        {
            bb[idx] = ((byte) ((length >> 8) & 0xFF));
            bb[idx + 1] = ((byte) (length & 0xFF));
        }
    }
}