//
//  Copyright (C) DataStax, Inc.
//
//  Please see the license for details:
//  http://www.datastax.com/terms/datastax-dse-driver-license-terms
//

using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace Dse.Test.Unit.Mapping.Pocos
{
    /// <summary>
    /// Represents an album that contains a UDT (Song)
    /// </summary>
    public class Album
    {
        public Guid Id { get; set; }

        public string Name { get; set; }

        public DateTimeOffset PublishingDate { get; set; }

        public List<Song2> Songs { get; set; }
    }
}
