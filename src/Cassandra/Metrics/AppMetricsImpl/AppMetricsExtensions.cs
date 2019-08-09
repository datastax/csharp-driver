// 
//       Copyright (C) 2019 DataStax Inc.
// 
//    Licensed under the Apache License, Version 2.0 (the "License");
//    you may not use this file except in compliance with the License.
//    You may obtain a copy of the License at
// 
//       http://www.apache.org/licenses/LICENSE-2.0
// 
//    Unless required by applicable law or agreed to in writing, software
//    distributed under the License is distributed on an "AS IS" BASIS,
//    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//    See the License for the specific language governing permissions and
//    limitations under the License.
// 

#if NETSTANDARD2_0
using System;
using App.Metrics;
using Cassandra.Metrics.DriverAbstractions;

namespace Cassandra.Metrics.AppMetricsImpl
{
    internal static class AppMetricsExtensions
    {
        public static Unit ToAppMetricsUnit(this DriverMeasurementUnit measurementUnit)
        {
            switch (measurementUnit)
            {
                case DriverMeasurementUnit.Bytes:
                    return Unit.Bytes;
                case DriverMeasurementUnit.Errors:
                    return Unit.Errors;
                case DriverMeasurementUnit.Requests:
                    return Unit.Requests;
                case DriverMeasurementUnit.Connections:
                    return Unit.Connections;
                case DriverMeasurementUnit.None:
                    return Unit.None;
                default:
                    throw new ArgumentOutOfRangeException(nameof(measurementUnit), measurementUnit, null);
            }
        }
    }
}
#endif