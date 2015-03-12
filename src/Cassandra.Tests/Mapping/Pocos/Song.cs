using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace Cassandra.Tests.Mapping.Pocos
{
    public class Song
    {
        public Guid Id { get; set; }

        public string Title { get; set; }

        public string Artist { get; set; }

        public DateTimeOffset ReleaseDate { get; set; }
    }

    /// <summary>
    /// A song class with the release date as nullable DateTime
    /// </summary>
    public class Song2
    {
        public Guid Id { get; set; }

        public string Title { get; set; }

        public string Artist { get; set; }

        public DateTime? ReleaseDate { get; set; }
    }
}
