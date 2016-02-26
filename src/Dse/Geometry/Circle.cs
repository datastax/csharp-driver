using System;
using System.Collections.Generic;
using System.Linq;
using System.Runtime.Serialization;
using System.Text;

namespace Dse.Geometry
{
    /// <summary>
    /// Represents the circle simple shape in Euclidean geometry composed by a center and a distance to the center (radius).
    /// </summary>
    [Serializable]
    public class Circle : GeometryBase
    {
        /// <summary>
        /// Gets the center of the circle.
        /// The centre of a circle is the point equidistant from the points on the edge.
        /// </summary>
        public Point Center { get; private set; }

        /// <summary>
        /// Returns the scalar value of the distance to the center.
        /// </summary>
        public double Radius { get; private set; }

        /// <summary>
        /// Creates a new instance of <see cref="Circle"/>.
        /// </summary>
        public Circle(Point center, double radius)
        {
            if (center == null)
            {
                throw new ArgumentNullException("center");
            }
            Center = center;
            Radius = radius;
        }

        /// <summary>
        /// Creates a new instance of <see cref="Circle"/>.
        /// </summary>
        protected Circle(SerializationInfo info, StreamingContext context)
        {
            var coordinates = (double[])info.GetValue("coordinates", typeof(double[]));
            Center = new Point(coordinates[0], coordinates[1]);
            Radius = Convert.ToDouble(info.GetValue("radius", typeof(double)));
        }

        /// <summary>
        /// Returns a value indicating whether this instance and a specified object represent the same value.
        /// </summary>
        public override bool Equals(object obj)
        {
            var other = obj as Circle;
            if (other == null)
            {
                return false;
            }
            return Center.Equals(other.Center) && Radius.Equals(other.Radius);
        }

        /// <summary>
        /// Returns the hash code based on the value of this instance.
        /// </summary>
        public override int GetHashCode()
        {
            // ReSharper disable NonReadonlyMemberInGetHashCode
            return CombineHashCode(new object[] { Center, Radius});
            // ReSharper enable NonReadonlyMemberInGetHashCode
        }

        /// <inheritdoc />
        public override void GetObjectData(SerializationInfo info, StreamingContext context)
        {
            info.AddValue("type", "Circle");
            info.AddValue("coordinates", new[] { Center.X, Center.Y });
            info.AddValue("radius", Radius);
        }

        /// <summary>
        /// Returns Well-known text (WKT) representation of the geometry object.
        /// </summary>
        public override string ToString()
        {
            return string.Format("CIRCLE (({0} {1}) {2})", Center.X, Center.Y, Radius);
        }
    }
}
