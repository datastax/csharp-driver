using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace Cassandra.Tests.Mapping.Pocos
{
    public class UserDifferentPropTypes
    {
        public Guid UserId { get; set; }
        public string Name { get; set; }
        //Table age column is an int, this property should fail
        public Dictionary<string, string> Age { get; set; }
    }
}
