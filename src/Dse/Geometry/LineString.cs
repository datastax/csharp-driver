using System;
using System.Collections.Generic;
using System.Collections.ObjectModel;
using System.Linq;
using System.Runtime.Serialization;
using System.Text;

namespace Dse.Geometry
{
    /// <summary>
    /// Represents a one-dimensional object representing a sequence of points and the line segments connecting them.
    /// </summary>
    [Serializable]
    public class LineString : Geometry
    {
        /// <summary>
        /// Gets the read-only list of points describing the LineString.
        /// </summary>
        public IList<Point> Points { get; private set; }

        /// <summary>
        /// Creates a new instance of <see cref="LineString"/> using a sequence of points.
        /// </summary>
        public LineString(params Point[] points) : this((IList<Point>)points)
        {

        }

        protected LineString(SerializationInfo info, StreamingContext context)
        {
            var coordinates = (double[][])info.GetValue("coordinates", typeof(double[][]));
            Points = AsReadOnlyCollection(coordinates.Select(arr => new Point(arr[0], arr[1])).ToArray());
        }

        /// <summary>
        /// Creates a new instance of <see cref="LineString"/> using a list of points.
        /// </summary>
        public LineString(IList<Point> points)
        {
            if (points == null)
            {
                throw new ArgumentNullException("points");
            }
            if (points.Count == 1)
            {
                throw new ArgumentOutOfRangeException("points", "LineString can be either empty or contain 2 or more points");
            }
            Points = AsReadOnlyCollection(points);
        }

        public override bool Equals(object obj)
        {
            var other = obj as LineString;
            if (other == null)
            {
                return false;
            }
            if (Points.Count != other.Points.Count)
            {
                return false;
            }
            return !(Points.Where((t, i) => !t.Equals(other.Points[i])).Any());
        }

        public override int GetHashCode()
        {
            return CombineHashCode(Points);
        }

        public override void GetObjectData(SerializationInfo info, StreamingContext context)
        {
            info.AddValue("type", "LineString");
            info.AddValue("coordinates", Points.Select(p => new [] { p.X, p.Y }));
        }

        /// <summary>
        /// Returns Well-known text (WKT) representation of the geometry object.
        /// </summary>
        public override string ToString()
        {
            if (Points.Count == 0)
            {
                return "LINESTRING EMPTY";
            }
            return string.Format("LINESTRING ({0})", string.Join(", ", Points.Select(p => p.X + " " + p.Y)));
        }
    }
}
