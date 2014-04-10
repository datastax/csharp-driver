//
//      Copyright (C) 2012 DataStax Inc.
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
using System.Diagnostics;
using System.Security;
using System.Threading;

namespace Cassandra
{
    internal static class CassandraCounters
    {
        private const string CassandraCountersCategory = "DataStax Cassandra C# driver";
        private static readonly Logger Logger = new Logger(typeof (CassandraCounters));
        private static readonly BoolSwitch CategoryReady = new BoolSwitch();

        #region CqlQueryCountPerSec

        private const string CqlQueryCountPerSecName = "Number of executed CQL queries per second";
        private static AtomicValue<PerformanceCounter> _cqlQueryCountPerSec = new AtomicValue<PerformanceCounter>(null);

        public static void IncrementCqlQueryCount()
        {
            SetupAndIncrement(ref _cqlQueryCountPerSec, CqlQueryCountPerSecName);
        }

        #endregion

        #region CqlQueryBeatsCounter

        private const string CqlQueryBeatsName = "Average CQL query execution time between samples(in nanoseconds).";

        private const string CqlQueryBeatsNameBase = "Base average CQL query execution time between samples";
        private static AtomicValue<PerformanceCounter> _cqlQueryBeats = new AtomicValue<PerformanceCounter>(null);
        private static AtomicValue<PerformanceCounter> _cqlQueryBeatsBase = new AtomicValue<PerformanceCounter>(null);

        public static void IncrementCqlQueryBeats(long value)
        {
            SetupAndIncrement(ref _cqlQueryBeats, CqlQueryBeatsName, value);
        }

        public static void IncrementCqlQueryBeatsBase()
        {
            SetupAndIncrement(ref _cqlQueryBeatsBase, CqlQueryBeatsNameBase);
        }

        private static void SetupAndIncrement(ref AtomicValue<PerformanceCounter> counter, string counterName, long? value = null,
                                              bool rawValue = false)
        {
            if (Diagnostics.CassandraPerformanceCountersEnabled)
            {
                SetupCounter(ref counter, CassandraCountersCategory, counterName);

                if (value != null)
                    if (rawValue)
                        counter.Value.RawValue = (long) value;
                    else
                        counter.Value.IncrementBy((long) value);
                else
                    counter.Value.Increment();
            }
        }

        #endregion

        #region QueryTimeRollingArithmeticAvrgCounter

        private const string QueryTimeRollingAvrgName = "Rolling arithmetic average time of CQL queries(in nanoseconds)";

        private const string QueryTimeRollingAvrgNameBase = "Base rolling arithmetic average time of CQL queries";

        private const int StepOfRollingAvrg = 30;
        private static AtomicValue<PerformanceCounter> _queryTimeRollingAvrg = new AtomicValue<PerformanceCounter>(null);
        private static AtomicValue<PerformanceCounter> _queryTimeRollingAvrgBase = new AtomicValue<PerformanceCounter>(null);
        private static readonly long[] CircledBufferAvrg = new long[StepOfRollingAvrg];

        private static int _current;

        public static void UpdateQueryTimeRollingAvrg(long value)
        {
            if (Diagnostics.CassandraPerformanceCountersEnabled)
            {
                SetupCounter(ref _queryTimeRollingAvrg, CassandraCountersCategory, QueryTimeRollingAvrgName);
                SetupCounter(ref _queryTimeRollingAvrgBase, CassandraCountersCategory, QueryTimeRollingAvrgNameBase);

                _queryTimeRollingAvrg.Value.RawValue = GetRollingArithmeticAverage(value)/100;
                _queryTimeRollingAvrgBase.Value.RawValue = 1;
            }
        }

        private static long GetRollingArithmeticAverage(long value)
        {
            CircledBufferAvrg[Interlocked.Increment(ref _current)%StepOfRollingAvrg] = value;
            Thread.MemoryBarrier();
            double avg = 0;
            for (int i = 0; i < StepOfRollingAvrg; i++)
                avg += CircledBufferAvrg[i];
            return (long) Math.Floor(avg/StepOfRollingAvrg);
        }

        #endregion

        private static void SetupCounter(ref AtomicValue<PerformanceCounter> counter, string counterCategory, string counterName)
        {
            if (CategoryReady.TryTake())
                SetupCategory();

            if (counter.Value == null)
                lock (counter)
                    if (counter.Value == null)
                        counter.Value = new PerformanceCounter(counterCategory, counterName, false);
        }

        private static void SetupCategory()
        {
            // Create category if:
            //  1) It not exists.
            //  2) New counter was added. - this situation occurs when category is already set on the machine, but it lacks the newly added counter, so it have to be recreated.                        
            if (PerformanceCounterCategory.Exists(CassandraCountersCategory))
            {
                if (
                    PerformanceCounterCategory.CounterExists(CqlQueryBeatsName, CassandraCountersCategory) &&
                    PerformanceCounterCategory.CounterExists(CqlQueryCountPerSecName, CassandraCountersCategory) &&
                    //Add here every counter that will be used in driver, to check if currently existing category contains it.
                    PerformanceCounterCategory.CounterExists(QueryTimeRollingAvrgName, CassandraCountersCategory))
                {
                    Logger.Info(string.Format("Performance counters category '{0}' already exists and contains all requiered counters.",
                                               CassandraCountersCategory));
                    return;
                }
                try
                {
                    PerformanceCounterCategory.Delete(CassandraCountersCategory);
                    Logger.Info("Successfully deleted performance counters category: " + CassandraCountersCategory);
                }
                catch (UnauthorizedAccessException ex)
                {
                    Logger.Error(
                        string.Format("Cannot delete performance counters category '{0}' due to insufficient administrative rights.",
                                      CassandraCountersCategory), ex);
                    throw;
                }
            }

            var ccdc = new CounterCreationDataCollection();

            var cqlQueryCountPerSecData = new CounterCreationData
            {
                CounterName = CqlQueryCountPerSecName,
                CounterHelp = "Number of Cql queries that were executed per second.",
                CounterType = PerformanceCounterType.RateOfCountsPerSecond64
            };


            ccdc.Add(cqlQueryCountPerSecData);

            var cqlQueryBeatsData = new CounterCreationData
            {
                CounterName = CqlQueryBeatsName,
                CounterHelp = "Counter that measures average Cql query execution time(in nanoseconds) between consecutive samples.",
                CounterType = PerformanceCounterType.AverageTimer32
            };

            var cqlQueryBeatsBaseData = new CounterCreationData
            {
                CounterName = CqlQueryBeatsNameBase,
                CounterHelp = "",
                CounterType = PerformanceCounterType.AverageBase
            };

            ccdc.Add(cqlQueryBeatsData);
            ccdc.Add(cqlQueryBeatsBaseData);


            var queryTimeRollingAvrgData = new CounterCreationData
            {
                CounterName = QueryTimeRollingAvrgName,
                CounterHelp =
                    "Counter that measures rolling arithmetic average of queries execution time in nanoseconds. By default, step is set to 30 previous samples.",
                CounterType = PerformanceCounterType.RawFraction
            };

            var queryTimeRollingAvrgDataBaseData = new CounterCreationData
            {
                CounterName = QueryTimeRollingAvrgNameBase,
                CounterHelp = "",
                CounterType = PerformanceCounterType.RawBase
            };

            ccdc.Add(queryTimeRollingAvrgData);
            ccdc.Add(queryTimeRollingAvrgDataBaseData);


            try
            {
                PerformanceCounterCategory.Create(
                    CassandraCountersCategory,
                    "Performance counters for DataStax Cassandra C# driver",
                    PerformanceCounterCategoryType.SingleInstance,
                    ccdc);
                Logger.Info("Successfully created performance counters category: " + CassandraCountersCategory);
            }
            catch (SecurityException ex)
            {
                Logger.Error("Cannot create performance counters category due to insufficient administrative rights.", ex);
                throw;
            }
        }
    }
}