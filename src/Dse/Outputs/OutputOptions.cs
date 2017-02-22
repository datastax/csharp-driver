//
//  Copyright (C) 2017 DataStax, Inc.
//
//  Please see the license for details:
//  http://www.datastax.com/terms/datastax-dse-driver-license-terms
//

using System.Collections.Generic;

namespace Dse
{
    internal class OutputOptions : IOutput
    {
        private readonly Dictionary<string, string[]> _options;

        public System.Guid? TraceId { get; internal set; }

        public IDictionary<string, string[]> Options
        {
            get { return _options; }
        }

        internal OutputOptions(FrameReader reader)
        {
            _options = new Dictionary<string, string[]>();
            int n = reader.ReadUInt16();
            for (var i = 0; i < n; i++)
            {
                var k = reader.ReadString();
                var v = reader.ReadStringList();
                _options.Add(k, v);
            }
        }

        public void Dispose()
        {
        }
    }
}
