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
using System;
using System.Collections;
using System.Collections.Generic;
using System.Globalization;
using System.Linq;
using System.Runtime.Serialization;
using System.Text.RegularExpressions;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;

namespace Cassandra.Geometry
{
    /// <summary>
    /// Represents is a plane geometry figure that is bounded by a finite chain of straight line segments closing in a
    /// loop to form a closed chain or circuit.
    /// </summary>
    [Serializable]
    [JsonConverter(typeof(PolygonJsonConverter))]
    public class Polygon : GeometryBase
    {
        private static readonly Regex WktRegex = new Regex(
            @"^POLYGON ?\((\(.*\))\)$", RegexOptions.Compiled);

        private IList<IList<Point>> _ringsWithOrderedPoints;

        /// <summary>
        /// A read-only list describing the rings of the polygon.
        /// </summary>
        public IList<IList<Point>> Rings { get; private set; }

        /// <inheritdoc />
        protected override IEnumerable GeoCoordinates
        {
            get { return Rings.Select(r => r.Select(p => new[] { p.X, p.Y })); }
        }

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
            _ringsWithOrderedPoints = Rings.Select(r => (IList<Point>) r.OrderBy(p => p).ToList()).ToList();
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
            _ringsWithOrderedPoints = Rings.Select(r => (IList<Point>) r.OrderBy(p => p).ToList()).ToList();
        }
        
        internal Polygon(JObject obj)
        {
            var coordinates = obj.GetValue("coordinates").ToObject<double[][][]>();
            Rings = AsReadOnlyCollection(coordinates
                                         .Select(r => (IList<Point>)r.Select(p => new Point(p[0], p[1])).ToList())
                                         .ToList());
            _ringsWithOrderedPoints = Rings.Select(r => (IList<Point>) r.OrderBy(p => p).ToList()).ToList();
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
                var r1 = _ringsWithOrderedPoints[i];
                var r2 = other._ringsWithOrderedPoints[i];
                if (!r1.SequenceEqual(r2))
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
            return CombineHashCode(_ringsWithOrderedPoints.Select(r => CombineHashCode(r.Select(p => p.GetHashCode()))));
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
                    string.Join(", ", r.Select(p => p.X.ToString(CultureInfo.InvariantCulture) + " " + p.Y.ToString(CultureInfo.InvariantCulture))) + 
                    ")")));
        }

        /// <summary>
        /// Creates a <see cref="Polygon"/> instance from a 
        /// <see href="https://en.wikipedia.org/wiki/Well-known_text">Well-known Text(WKT)</see>
        /// representation of a polygon.
        /// </summary>
        public static Polygon Parse(string textValue)
        {
            if (textValue == null)
            {
                throw new ArgumentNullException("textValue");
            }
            if (textValue == "POLYGON EMPTY")
            {
                return new Polygon();
            }
            Action<bool> validateWkt = condition =>
            {
                if (condition)
                {
                    throw InvalidFormatException(textValue);
                }
            };
            var match = WktRegex.Match(textValue);
            validateWkt(!match.Success || match.Groups.Count != 2);
            var ringsText = match.Groups[1].Value;
            var ringsArray = new LinkedList<string>();
            var ringStart = -1;
            for (var i = 0; i < ringsText.Length; i++)
            {
                var c = ringsText[i];
                if (c == '(')
                {
                    validateWkt(ringStart != -1);
                    ringStart = i + 1;
                    continue;
                }
                if (c == ')')
                {
                    validateWkt(ringStart == -1);
                    ringsArray.AddLast(ringsText.Substring(ringStart, i - ringStart));
                    ringStart = -1;
                    continue;
                }
                validateWkt(ringStart == -1 && c != ' ' && c != ',');
            }
            var lines = ringsArray.Select(r => (IList<Point>)LineString.ParseSegments(r)).ToList();
            return new Polygon(lines);
        }
    }
}
