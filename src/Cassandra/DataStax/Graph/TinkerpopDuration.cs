//
//      Copyright (C) DataStax Inc.
//
//   Licensed under the Apache License, Version 2.0 (the "License");
//   you may not use this file except in compliance with the License.
//   You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
//   Unless required by applicable law or agreed to in writing, software
//   distributed under the License is distributed on an "AS IS" BASIS,
//   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//   See the License for the specific language governing permissions and
//   limitations under the License.
//

using System;

namespace Cassandra.DataStax.Graph
{
    // TODO GRAPH
    ///// <summary>
    ///// Represents a value of type "gx:Duration" in DSE Graph. Note that <see cref="Duration"/> represents the CQL Duration type.
    ///// It's basically a wrapper around a <see cref="Duration"/> value.
    ///// </summary>
    //public struct TinkerpopDuration : IEquatable<TinkerpopDuration>, IEquatable<TimeSpan>, IEquatable<Duration>
    //{
    //    public TinkerpopDuration(Duration duration)
    //    {
    //        Value = duration;
    //    }

    //    public Duration AsDuration() => (Duration) this;

    //    public TimeSpan AsTimeSpan() => (TimeSpan) this;

    //    public Duration Value { get; }
        
    //    public static implicit operator TinkerpopDuration(Duration obj) => new TinkerpopDuration(obj);

    //    public static implicit operator Duration(TinkerpopDuration obj) => obj.Value;
        
    //    public static explicit operator TimeSpan(TinkerpopDuration obj) => obj.Value.ToTimeSpan();
        
    //    public static explicit operator TinkerpopDuration(TimeSpan obj) => new TinkerpopDuration(Duration.FromTimeSpan(obj));

    //    public static bool operator ==(TinkerpopDuration i1, TinkerpopDuration i2)
    //    {
    //        return i1.Equals(i2);
    //    }

    //    public static bool operator !=(TinkerpopDuration i1, TinkerpopDuration i2)
    //    {
    //        return !(i1 == i2);
    //    }
        
    //    public static bool operator ==(TinkerpopDuration i1, Duration i2)
    //    {
    //        return i1.Equals(i2);
    //    }

    //    public static bool operator !=(TinkerpopDuration i1, Duration i2)
    //    {
    //        return !(i1 == i2);
    //    }
        
    //    public static bool operator ==(Duration i1, TinkerpopDuration i2)
    //    {
    //        return i2.Equals(i1);
    //    }

    //    public static bool operator !=(Duration i1, TinkerpopDuration i2)
    //    {
    //        return !(i1 == i2);
    //    }
        
    //    public static bool operator ==(TinkerpopDuration i1, TimeSpan i2)
    //    {
    //        return i1.Equals(i2);
    //    }

    //    public static bool operator !=(TinkerpopDuration i1, TimeSpan i2)
    //    {
    //        return !(i1 == i2);
    //    }
        
    //    public static bool operator ==(TimeSpan i1, TinkerpopDuration i2)
    //    {
    //        return i2.Equals(i1);
    //    }

    //    public static bool operator !=(TimeSpan i1, TinkerpopDuration i2)
    //    {
    //        return !(i1 == i2);
    //    }

    //    public bool Equals(TinkerpopDuration other)
    //    {
    //        return Value.Equals(other.Value);
    //    }

    //    public bool Equals(Duration other)
    //    {
    //        return AsDuration().Equals(other);
    //    }

    //    public bool Equals(TimeSpan other)
    //    {
    //        return AsTimeSpan().Equals(other);
    //    }

    //    public override int GetHashCode()
    //    {
    //        return -1937169414 + Value.GetHashCode();
    //    }

    //    public override bool Equals(object obj)
    //    {
    //        switch (obj)
    //        {
    //            case null:
    //                return false;
    //            case Duration d:
    //                return Equals(d);
    //            case TimeSpan ts:
    //                return Equals(ts);
    //            case TinkerpopDuration i:
    //                return Equals(i);
    //            default:
    //                return false;
    //        }
    //    }

    //    public override string ToString()
    //    {
    //        return Value.ToString();
    //    }
    //}
}