//
//  Copyright (C) 2017 DataStax, Inc.
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

    /// <summary>
    /// A song class with 2 constructors
    /// </summary>
    public class Song3
    {
        public Song3()
        {
            
        }

        public Song3(Guid id)
        {
            Id = id;
        }

        public Guid Id { get; set; }

        public string Title { get; set; }

        public string Artist { get; set; }

        public DateTimeOffset ReleaseYear { get; set; }

        public long Counter { get; set; }
    }
}
