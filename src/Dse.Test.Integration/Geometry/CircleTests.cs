using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Dse.Geometry;

namespace Dse.Test.Integration.Geometry
{
    public class CircleTests : GeometryTests<Circle>
    {
        protected override Circle[] Values
        {
            get
            {
                return new []
                {
                    new Circle(new Point(1.2, 3.9), 6.2),
                    new Circle(new Point(-1.2, 1.9), 111),
                    new Circle(new Point(0.21222, 32.9), 10.21222)
                };
            }
        }

        protected override string TypeName
        {
            get { return "CircleType"; }
        }
    }
}
