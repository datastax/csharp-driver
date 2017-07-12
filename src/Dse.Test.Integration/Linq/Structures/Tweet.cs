//
//  Copyright (C) 2017 DataStax, Inc.
//
//  Please see the license for details:
//  http://www.datastax.com/terms/datastax-dse-driver-license-terms
//

using System;
using Dse.Data.Linq;
using System.Diagnostics;
#pragma warning disable 618

namespace Dse.Test.Integration.Linq.Structures
{
    public class Tweet
    {
        public string AuthorId;
        public string Body;
        public Guid TweetId;
    }
}
