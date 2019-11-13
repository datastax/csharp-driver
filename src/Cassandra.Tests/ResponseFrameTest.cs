//
//  Copyright (C) DataStax, Inc.
//
//  Please see the license for details:
//  http://www.datastax.com/terms/datastax-dse-driver-license-terms
//

using System;
using System.IO;
using Cassandra.Serialization;
using NUnit.Framework;

namespace Cassandra.Tests
{
    public class ResponseFrameTest
    {
        [Test]
        public void Ctor_HeaderIsNull_Throws()
        {
            // ReSharper disable once ObjectCreationAsStatement
            Assert.Throws<ArgumentNullException>(() => new Frame(
                null, new MemoryStream(), new Serializer(ProtocolVersion.MaxSupported)));
        }

        [Test]
        public void Ctor_BodyIsNull_Throws()
        {
            // ReSharper disable once ObjectCreationAsStatement
            Assert.Throws<ArgumentNullException>(() => new Frame(
                new FrameHeader(), null, new Serializer(ProtocolVersion.MaxSupported)));
        } 
    }
}