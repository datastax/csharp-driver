using System;
using System.Collections.Generic;
using System.Linq;
using System.Runtime.Serialization;
using System.Text;

namespace Dse.Geometry
{
    /// <summary>
    /// Represents is a plane geometry figure that is bounded by a finite chain of straight line segments closing in a
    /// loop to form a closed chain or circuit.
    /// </summary>
    [Serializable]
    public class Polygon : GeometryBase
    {
        /// <summary>
        /// A read-only list describing the rings of the polygon.
        /// </summary>
        public IList<IList<Point>> Rings { get; private set; }

        /// <summary>
        /// Creates a new instance of <see cref="Polygon"/> with a single ring.
        /// </summary>
        /// <param name="points">The points of the single ring</param>
        public Polygon(params Point[] points)
            : this((IList<IList<Point>>) new[] { (IList<Point>)points })
        {

        }

        /// <summary>
        /// Creates a new instance of <see cref="Polygon"/> with a sequence of rings.
        /// </summary>
        /// <param name="points">The points of the single ring</param>
        public Polygon(params IList<Point>[] points)
            : this((IList<IList<Point>>) points)
        {

        }

        /// <summary>
        /// Creates a new instance of <see cref="Polygon"/> with no rings (empty).
        /// </summary>
        public Polygon() : this((IList<IList<Point>>) new IList<Point>[0])
        {
            
        }

        /// <summary>
        /// Creates a new instance of <see cref="Polygon"/> using multiple rings.
        /// </summary>
        /// <param name="rings">The polygon rings</param>
        public Polygon(IList<IList<Point>> rings)
        {
            if (rings == null)
            {
                throw new ArgumentNullException("rings");
            }
            Rings = AsReadOnlyCollection(rings, r => AsReadOnlyCollection(r));
        }

        /// <summary>
        /// Creates a new instance of <see cref="Polygon"/> using serialization information.
        /// </summary>
        protected Polygon(SerializationInfo info, StreamingContext context)
        {
            var coordinates = (double[][][])info.GetValue("coordinates", typeof(double[][][]));
            Rings = AsReadOnlyCollection(coordinates
                .Select(r => (IList<Point>)r.Select(p => new Point(p[0], p[1])).ToList())
                .ToList());
        }

        /// <summary>
        /// Returns a value indicating whether this instance and a specified object represent the same value.
        /// </summary>
        public override bool Equals(object obj)
        {
            var other = obj as Polygon;
            if (other == null)
            {
                return false;
            }
            if (Rings.Count != other.Rings.Count)
            {
                return false;
            }
            for (var i = 0; i < Rings.Count; i++)
            {
                var r1 = Rings[i];
                var r2 = other.Rings[i];
                if (r1.Where((p, j) => !p.Equals(r2[j])).Any())
                {
                    return false;
                }
            }
            return true;
        }

        /// <summary>
        /// Returns the hash code based on the value of this instance.
        /// </summary>
        public override int GetHashCode()
        {
            // ReSharper disable once NonReadonlyMemberInGetHashCode
            return CombineHashCode(Rings.Select(r => CombineHashCode(r.Select(p => p.GetHashCode()))));
        }

        /// <inheritdoc />
        public override void GetObjectData(SerializationInfo info, StreamingContext context)
        {
            info.AddValue("type", "Polygon");
            info.AddValue("coordinates", Rings.Select(r => r.Select(p => new [] { p.X, p.Y})));
        }

        /// <summary>
        /// Returns Well-known text (WKT) representation of the geometry object.
        /// </summary>
        public override string ToString()
        {
            if (Rings.Count == 0)
            {
                return "POLYGON EMPTY";
            }
            return string.Format("POLYGON ({0})", string.Join(", ", 
                Rings.Select(r => 
                    "(" + 
                    string.Join(", ", r.Select(p => p.X + " " + p.Y)) + 
                    ")")));
        }
    }
}
