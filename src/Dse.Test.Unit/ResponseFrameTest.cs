//
//  Copyright (C) DataStax, Inc.
//
//  Please see the license for details:
//  http://www.datastax.com/terms/datastax-dse-driver-license-terms
//

using System;
using System.IO;
using Dse.Serialization;
using NUnit.Framework;

namespace Dse.Test.Unit
{
    public class ResponseFrameTest
    {
        [Test]
        public void Ctor_HeaderIsNull_Throws()
        {
            // ReSharper disable once ObjectCreationAsStatement
            Assert.Throws<ArgumentNullException>(() => new Frame(
                null, new MemoryStream(), new SerializerManager(ProtocolVersion.MaxSupported).GetCurrentSerializer()));
        }

        [Test]
        public void Ctor_BodyIsNull_Throws()
        {
            // ReSharper disable once ObjectCreationAsStatement
            Assert.Throws<ArgumentNullException>(() => new Frame(
                new FrameHeader(), null, new SerializerManager(ProtocolVersion.MaxSupported).GetCurrentSerializer()));
        } 
    }
}