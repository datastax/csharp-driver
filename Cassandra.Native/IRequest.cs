using System;
using System.Collections.Generic;
using System.Text;

namespace Cassandra
{
    internal interface IRequest
    {
        RequestFrame GetFrame();
    }
}
