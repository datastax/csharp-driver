using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace Cassandra.Tests.Mapping.Pocos
{
    /// <summary>
    /// Represents an album that contains a UDT (Song)
    /// </summary>
    public class Album
    {
        public Guid Id { get; set; }

        public string Name { get; set; }

        public DateTimeOffset PublishingDate { get; set; }

        public List<Song> Songs { get; set; }
    }
}
