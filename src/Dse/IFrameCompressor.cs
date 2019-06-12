//
//  Copyright (C) DataStax, Inc.
//
//  Please see the license for details:
//  http://www.datastax.com/terms/datastax-dse-driver-license-terms
//

using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;

namespace Dse
{
    /// <summary>
    /// Defines the methods for frame compression and decompression
    /// </summary>
    public interface IFrameCompressor
    {
        /// <summary>
        /// Creates and returns stream (clear text) using the provided compressed <c>stream</c> as input.
        /// </summary>
        Stream Decompress(Stream stream);
    }
}
