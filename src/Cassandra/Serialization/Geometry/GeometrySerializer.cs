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

namespace Cassandra.Serialization.Geometry
{
    internal abstract class GeometrySerializer<T> : TypeSerializer<T>
    {
        /// <summary>
        /// WKB geometry type codes
        /// </summary>
        internal enum GeometryType
        {
            Point2D = 1,
            LineString = 2,
            Polygon = 3,
            Circle = 101
        }

        /// <summary>
        /// Returns true if the buffer is little endian according to WKB.
        /// </summary>
        protected bool IsLittleEndian(byte[] buffer, int offset)
        {
            return (buffer[offset] == 1);
        }

        /// <summary>
        /// Returns true is the CPU is little-endian.
        /// </summary>
        protected virtual bool UseLittleEndianSerialization()
        {
            // instead of using CPU endianness, we hardcode it to LE.
            // see DSP-10092
            return true;
        }
    }
}
