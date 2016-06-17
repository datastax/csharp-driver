//
//  Copyright (C) 2016 DataStax, Inc.
//
//  Please see the license for details:
//  http://www.datastax.com/terms/datastax-dse-driver-license-terms
//
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Cassandra.Serialization;

namespace Dse.Geometry
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
        protected virtual   bool IsCpuLittleEndian()
        {
            return BitConverter.IsLittleEndian;
        }
    }
}
