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
﻿using System;
using System.Diagnostics;
using System.Threading;

namespace Cassandra
{    
    internal static class CassandraCounters
    {
        private static readonly Logger _logger = new Logger(typeof(CassandraCounters));
        private static bool CategoryReady = false;
        private const string CassandraCountersCategory = "DataStax Cassandra C# driver";

        #region CqlQueryCountPerSec
        [ThreadStatic]
        static PerformanceCounter CqlQueryCountPerSec = null;
        private const string CqlQueryCountPerSecName = "Number of executed CQL queries per second";

        public static void IncrementCqlQueryCount()
        {
            setupAndIncrement(CqlQueryCountPerSec, CqlQueryCountPerSecName);
        }
        
        #endregion

        #region CqlQueryBeatsCounter

        [ThreadStatic]
        static PerformanceCounter CqlQueryBeats = null;
        private const string CqlQueryBeatsName = "Average CQL query execution time between samples(in nanoseconds).";

        [ThreadStatic]
        static PerformanceCounter CqlQueryBeatsBase = null;
        private const string CqlQueryBeatsNameBase = "Base average CQL query execution time between samples";

        public static void IncrementCqlQueryBeats(long value)
        {
            setupAndIncrement(CqlQueryBeats, CqlQueryBeatsName, value);
        }

        public static void IncrementCqlQueryBeatsBase()
        {
            setupAndIncrement(CqlQueryBeatsBase, CqlQueryBeatsNameBase);
        }

        private static void setupAndIncrement(PerformanceCounter counter, string counterName, long? value = null)
        {
            if (Diagnostics.CassandraPerformanceCountersEnabled)
            {
                if (counter == null)
                    counter = SetupCounter(CassandraCountersCategory, counterName);

                if (counter != null && CategoryReady)
                    if (value != null)
                        counter.IncrementBy((long)value);
                    else
                        counter.Increment();
            }
        }
        #endregion

        #region QueryTimeRollingArithmeticAvrgCounter

        [ThreadStatic]
        static PerformanceCounter QueryTimeRollingAvrg = null;
        private const string QueryTimeRollingAvrgName = "Rolling arithmetic average time of CQL queries(in nanoseconds)";

        [ThreadStatic]
        static PerformanceCounter QueryTimeRollingAvrgBase = null;
        private const string QueryTimeRollingAvrgNameBase = "Base rolling arithmetic average time of CQL queries";

        public static void UpdateQueryTimeRollingAvrg(long value)
        {
            if (Diagnostics.CassandraPerformanceCountersEnabled)
            {
                if (QueryTimeRollingAvrg == null)
                    QueryTimeRollingAvrg = SetupCounter(CassandraCountersCategory, QueryTimeRollingAvrgName);

                if (QueryTimeRollingAvrgBase == null)                
                    QueryTimeRollingAvrgBase = SetupCounter(CassandraCountersCategory, QueryTimeRollingAvrgNameBase);                    
                
                if (QueryTimeRollingAvrg != null && QueryTimeRollingAvrgBase != null && CategoryReady)
                {
                    QueryTimeRollingAvrg.RawValue = getRollingArithmeticAverage(value)/100;
                    QueryTimeRollingAvrgBase.RawValue = 1;
                }                    
            }
        }

        private const int stepOfRollingAvrg = 30;
        private static long[] circledBufferAvrg = new long[stepOfRollingAvrg] { 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0 };
        
        private static int current = 0;
        
        private static long getRollingArithmeticAverage(long value)
        {
            circledBufferAvrg[Interlocked.Increment(ref current) % stepOfRollingAvrg] = value;
            Thread.MemoryBarrier();
            double avg = 0;
            for (int i = 0; i < stepOfRollingAvrg; i++)
                avg += circledBufferAvrg[i];
            return current >= stepOfRollingAvrg ? (long)Math.Floor(avg / stepOfRollingAvrg) : 0;
        }
        #endregion


        private static object guard = new object();
        private static PerformanceCounter SetupCounter(string counterCategory, string counterName)
        {
            if (!CategoryReady)
                lock (guard)
                    if (!CategoryReady) SetupCategory();

            return CategoryReady ? new PerformanceCounter(counterCategory, counterName, false) : null;
        }

        private static void SetupCategory()
        {
            // Create category if:
            //  1) It not exists.
            //  2) New counter was added. - this situation occurs when category is already set on the machine, but it lacks the newly added counter, so it have to be recreated.                        
            if (PerformanceCounterCategory.Exists(CassandraCountersCategory))
                if (PerformanceCounterCategory.CounterExists(CqlQueryBeatsName, CassandraCountersCategory) &&
                    PerformanceCounterCategory.CounterExists(CqlQueryCountPerSecName, CassandraCountersCategory) &&//Add here every counter that will be used in driver, to check if currently existing category contains it.
                    PerformanceCounterCategory.CounterExists(QueryTimeRollingAvrgName, CassandraCountersCategory)) 
                {
                    CategoryReady = true;
                    _logger.Info(string.Format("Performance counters category '{0}' already exists and contains all requiered counters.", CassandraCountersCategory));
                    return;
                }
                else
                    try
                    {
                        PerformanceCounterCategory.Delete(CassandraCountersCategory);
                        _logger.Info("Successfully deleted performance counters category: " + CassandraCountersCategory);
                    }
                    catch (System.UnauthorizedAccessException ex)
                    {                        
                        _logger.Error(string.Format("Cannot delete performance counters category '{0}' due to insufficient administrative rights.", CassandraCountersCategory), ex);
                        throw;
                    }


            CounterCreationDataCollection CCDC = new CounterCreationDataCollection();

            CounterCreationData CqlQueryCountPerSecData = new CounterCreationData()
            {
                CounterName = CqlQueryCountPerSecName,
                CounterHelp = "Number of Cql queries that were executed per second.",
                CounterType = PerformanceCounterType.RateOfCountsPerSecond64
            };


            CCDC.Add(CqlQueryCountPerSecData);

            
            CounterCreationData CqlQueryBeatsData = new CounterCreationData()
            {
                CounterName = CqlQueryBeatsName,
                CounterHelp = "Counter that measures average Cql query execution time(in nanoseconds) between consecutive samples.",
                CounterType = PerformanceCounterType.AverageTimer32
            };

            CounterCreationData CqlQueryBeatsBaseData = new CounterCreationData()
            {
                CounterName = CqlQueryBeatsNameBase,
                CounterHelp = "",
                CounterType = PerformanceCounterType.AverageBase
            };

            CCDC.Add(CqlQueryBeatsData);
            CCDC.Add(CqlQueryBeatsBaseData);


            CounterCreationData QueryTimeRollingAvrgData = new CounterCreationData()
            {
                CounterName = QueryTimeRollingAvrgName,
                CounterHelp = "Counter that measures rolling arithmetic average of queries execution time in nanoseconds. By default, step is set to 30 previous samples.",
                CounterType = PerformanceCounterType.RawFraction
            };

            CounterCreationData QueryTimeRollingAvrgDataBaseData = new CounterCreationData()
            {
                CounterName = QueryTimeRollingAvrgNameBase,
                CounterHelp = "",
                CounterType = PerformanceCounterType.RawBase
            };

            CCDC.Add(QueryTimeRollingAvrgData);
            CCDC.Add(QueryTimeRollingAvrgDataBaseData);


            try
            {
                PerformanceCounterCategory.Create(
                                                    CassandraCountersCategory,
                                                    "Performance counters for DataStax Cassandra C# driver",
                                                    PerformanceCounterCategoryType.SingleInstance,
                                                    CCDC);
                _logger.Info("Successfully created performance counters category: " + CassandraCountersCategory);
                CategoryReady = true;
            }
            catch (System.Security.SecurityException ex)
            {
                _logger.Error("Cannot create performance counters category due to insufficient administrative rights.", ex);
                throw;
            }
        }
    }

}
