//
//       Copyright (C) DataStax, Inc.
//
//     Please see the license for details:
//     http://www.datastax.com/terms/datastax-dse-driver-license-terms
//

using System.Collections.Generic;

namespace Dse.Insights.Schema.Converters
{
    internal class CompressionTypeInsightsConverter : InsightsEnumConverter<CompressionType, string>
    {
        private static readonly IReadOnlyDictionary<CompressionType, string> CompressionTypeStringMap =
            new Dictionary<CompressionType, string>
            {
                { CompressionType.LZ4, "LZ4" },
                { CompressionType.NoCompression, "NONE" },
                { CompressionType.Snappy, "SNAPPY" }
            };
        
        protected override IReadOnlyDictionary<CompressionType, string> EnumToJsonValueMap =>
            CompressionTypeInsightsConverter.CompressionTypeStringMap;
    }
}