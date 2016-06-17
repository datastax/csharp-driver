//
//  Copyright (C) 2016 DataStax, Inc.
//
//  Please see the license for details:
//  http://www.datastax.com/terms/datastax-dse-driver-license-terms
//
using System;
using System.Collections.Generic;
using System.Linq;
using System.Runtime.Serialization;
using System.Text;

namespace Dse.Geometry
{
    /// <summary>
    /// Represents a zero-dimensional object that represents a specific (X,Y) location in a two-dimensional XY-Plane.
    /// In case of Geographic Coordinate Systems, the X coordinate is the longitude and the Y is the latitude.
    /// </summary>
    [Serializable]
    public class Point : GeometryBase
    {
        /// <summary>
        /// Returns the X coordinate of this 2D point.
        /// </summary>
        public double X { get; private set; }

        /// <summary>
        /// Returns the Y coordinate of this 2D point.
        /// </summary>
        public double Y { get; private set; }

        /// <summary>
        /// Creates a new instance of <see cref="Point"/>.
        /// </summary>
        /// <param name="x">The <c>X</c> coordinate.</param>
        /// <param name="y">The <c>Y</c> coordinate.</param>
        public Point(double x, double y)
        {
            X = x;
            Y = y;
        }

        /// <summary>
        /// Creates a new instance of <see cref="Point"/>.
        /// </summary>
        protected Point(SerializationInfo info, StreamingContext context)
        {
            var coordinates = (double[])info.GetValue("coordinates", typeof(double[]));
            X = coordinates[0];
            Y = coordinates[1];
        }

        /// <summary>
        /// Returns a value indicating whether this instance and a specified object represent the same value.
        /// </summary>
        public override bool Equals(object obj)
        {
            var other = obj as Point;
            if (other == null)
            {
                return false;
            }
            return X.Equals(other.X) && Y.Equals(other.Y);
        }

        /// <inheritdoc />
        public override int GetHashCode()
        {
            // ReSharper disable NonReadonlyMemberInGetHashCode
            return CombineHashCode(new [] { X, Y});
            // ReSharper enable NonReadonlyMemberInGetHashCode
        }

        /// <inheritdoc />
        public override void GetObjectData(SerializationInfo info, StreamingContext context)
        {
            info.AddValue("type", "Point");
            info.AddValue("coordinates", new [] { X, Y });
        }

        /// <summary>
        /// Returns Well-known text (WKT) representation of the geometry object.
        /// </summary>
        public override string ToString()
        {
            return string.Format("POINT ({0} {1})", X, Y);
        }
    }
}
