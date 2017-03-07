//
//  Copyright (C) 2017 DataStax, Inc.
//
//  Please see the license for details:
//  http://www.datastax.com/terms/datastax-dse-driver-license-terms
//

using System;

namespace Dse
{
    public class SchemaChangedEventArgs : EventArgs
    {
        public enum Kind
        {
            Created,
            Dropped,
            Updated
        }

        public string Keyspace;
        public string Table;
        public Kind What;
    }
}