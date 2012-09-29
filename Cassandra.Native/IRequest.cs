using System;
using System.Collections.Generic;
using System.Text;

namespace Cassandra.Native
{
    internal interface IRequest
    {
        RequestFrame GetFrame();
    }
}
