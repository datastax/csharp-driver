//
//  Copyright (C) DataStax, Inc.
//
//  Please see the license for details:
//  http://www.datastax.com/terms/datastax-dse-driver-license-terms
//

using System;
using System.IO;
using Dse.Serialization;

namespace Dse
{
    internal class Frame
    {
        private readonly Stream _body;
        private readonly ISerializer _serializer;
        private readonly FrameHeader _header;

        /// <summary>
        /// The 8 byte protocol header
        /// </summary>
        public FrameHeader Header
        {
            get { return _header; }
        }

        /// <summary>
        /// A stream containing the frame body
        /// </summary>
        public Stream Body
        {
            get { return _body; }
        }

        /// <summary>
        /// Gets the serializer instance to be used for this frame
        /// </summary>
        public ISerializer Serializer
        {
            get { return _serializer; }
        }

        public Frame(FrameHeader header, Stream body, ISerializer serializer)
        {
            if (header == null)
            {
                throw new ArgumentNullException("header");
            }
            if (body == null)
            {
                throw new ArgumentNullException("body");
            }
            if (serializer == null)
            {
                throw new ArgumentNullException("serializer");
            }

            _header = header;
            _body = body;
            _serializer = serializer;
        }
    }
}