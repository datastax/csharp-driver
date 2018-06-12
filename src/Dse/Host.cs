//
//  Copyright (C) 2017 DataStax, Inc.
//
//  Please see the license for details:
//  http://www.datastax.com/terms/datastax-dse-driver-license-terms
//

using System;
using System.Collections.Generic;
using System.Net;
using System.Threading;

namespace Dse
{
    /// <summary>
    /// Represents a Cassandra node.
    /// </summary>
    public class Host : IEquatable<Host>
    {
        private static readonly Logger Logger = new Logger(typeof(Host));
        private long _isUpNow = 1;
        private int _distance = (int) HostDistance.Ignored;
        private static readonly IReadOnlyCollection<string> WorkloadsDefault = new string[0];

        /// <summary>
        /// Event that gets raised when the host is set as DOWN (not available) by the driver, after being UP.
        /// It provides the delay for the next reconnection attempt.
        /// </summary>
        internal event Action<Host> Down;

        /// <summary>
        /// Event that gets raised when the host is considered back UP (available for queries) by the driver.
        /// </summary>
        internal event Action<Host> Up;

        /// <summary>
        /// Event that gets raised when the host is being decommissioned from the cluster.
        /// </summary>
        internal event Action Remove;

        /// <summary>
        /// Event that gets raised when there is a change in the distance, perceived by the host.
        /// </summary>
        internal event Action<HostDistance, HostDistance> DistanceChanged;

        /// <summary>
        /// Determines if the host is UP for the driver
        /// </summary>
        public bool IsUp
        {
            get { return Interlocked.Read(ref _isUpNow) == 1L; }
        }

        /// <summary>
        /// This property is going to be removed in future versions, use <see cref="IsUp"/> instead.
        /// Used to determines if the host can be considered as UP
        /// </summary>
        public bool IsConsiderablyUp
        {
            get { return IsUp; }
        }

        /// <summary>
        ///  Gets the node address.
        /// </summary>
        public IPEndPoint Address { get; }

        /// <summary>
        /// Tokens assigned to the host
        /// </summary>
        internal IEnumerable<string> Tokens { get; private set; }

        /// <summary>
        ///  Gets the name of the datacenter this host is part of. The returned
        ///  datacenter name is the one as known by Cassandra. Also note that it is
        ///  possible for this information to not be available. In that case this method
        ///  returns <c>null</c> and caller should always expect that possibility.
        /// </summary>
        public string Datacenter { get; internal set; }

        /// <summary>
        ///  Gets the name of the rack this host is part of. The returned rack name is
        ///  the one as known by Cassandra. Also note that it is possible for this
        ///  information to not be available. In that case this method returns
        ///  <c>null</c> and caller should always expect that possibility.
        /// </summary>
        public string Rack { get; private set; }

        /// <summary>
        /// The Cassandra version the host is running.
        /// <remarks>
        /// The value returned can be null if the information is unavailable.
        /// </remarks>
        /// </summary>
        public Version CassandraVersion { get; private set; }

        /// <summary>
        /// Gets the DSE Workloads the host is running.
        /// <para>
        ///   This is based on the "workload" or "workloads" columns in <c>system.local</c> and <c>system.peers</c>.
        /// </para>
        /// <para>
        ///   Workload labels may vary depending on the DSE version in use; e.g. DSE 5.1 may report two distinct
        ///   workloads: <c>Search</c> and <c>Analytics</c>, while DSE 5.0 would report a single
        ///   <c>SearchAnalytics</c> workload instead. The driver simply returns the workload labels as reported by
        ///   DSE, without any form of pre-processing.
        /// </para>
        /// <para>When the information is unavailable, this property returns an empty collection.</para>
        /// </summary>
        /// <remarks>Collection can be considered as immutable.</remarks>
        public IReadOnlyCollection<string> Workloads { get; private set; }

        /// <summary>
        /// Creates a new instance of <see cref="Host"/>.
        /// </summary>
        // ReSharper disable once UnusedParameter.Local : Part of the public API
        public Host(IPEndPoint address, IReconnectionPolicy reconnectionPolicy) : this(address)
        {

        }

        internal Host(IPEndPoint address)
        {
            Address = address ?? throw new ArgumentNullException(nameof(address));
            Workloads = WorkloadsDefault;
        }

        /// <summary>
        /// Sets the Host as Down.
        /// Returns false if it was already considered as Down by the driver.
        /// </summary>
        public bool SetDown()
        {
            var wasUp = Interlocked.CompareExchange(ref _isUpNow, 0, 1) == 1;
            if (!wasUp)
            {
                return false;
            }
            Logger.Warning("Host {0} considered as DOWN.", Address);
            if (Down != null)
            {
                Down(this);
            }
            return true;
        }

        /// <summary>
        /// Returns true if the host was DOWN and it was set as UP.
        /// </summary>
        public bool BringUpIfDown()
        {
            var wasUp = Interlocked.CompareExchange(ref _isUpNow, 1, 0) == 1;
            if (wasUp)
            {
                return false;
            }
            Logger.Info("Host {0} is now UP", Address);
            if (Up != null)
            {
                Up(this);
            }
            return true;
        }

        public void SetAsRemoved()
        {
            Logger.Info("Decommissioning node {0}", Address);
            Interlocked.Exchange(ref _isUpNow, 0);
            if (Remove != null)
            {
                Remove();
            }
        }

        /// <summary>
        /// Sets datacenter, rack and other basic information of a host.
        /// </summary>
        internal void SetInfo(IRow row)
        {
            Datacenter = row.GetValue<string>("data_center");
            Rack = row.GetValue<string>("rack");
            Tokens = row.GetValue<IEnumerable<string>>("tokens") ?? new string[0];

            if (row.ContainsColumn("release_version"))
            {
                var releaseVersion = row.GetValue<string>("release_version");
                if (releaseVersion != null)
                {
                    CassandraVersion = Version.Parse(releaseVersion.Split('-')[0]);
                }
            }

            if (row.ContainsColumn("workloads"))
            {
                Workloads = row.GetValue<string[]>("workloads");
            }
            else if (row.ContainsColumn("workload") && row.GetValue<string>("workload") != null)
            {
                Workloads = new[] { row.GetValue<string>("workload") };
            }
            else
            {
                Workloads = WorkloadsDefault;
            }
        }

        /// <summary>
        /// The hash value of the address of the host
        /// </summary>
        public override int GetHashCode()
        {
            return Address.GetHashCode();
        }

        /// <summary>
        /// Determines if the this instance can be considered equal to the provided host.
        /// </summary>
        public bool Equals(Host other)
        {
            return Equals(Address, other?.Address);
        }

        /// <inheritdoc />
        public override bool Equals(object obj)
        {
            if (ReferenceEquals(null, obj)) return false;
            if (ReferenceEquals(this, obj)) return true;
            if (obj.GetType() != this.GetType()) return false;
            return Equals((Host) obj);
        }

        /// <summary>
        /// Updates the internal state representing the distance.
        /// </summary>
        internal void SetDistance(HostDistance distance)
        {
            var previousDistance = (HostDistance) Interlocked.Exchange(ref _distance, (int)distance);
            if (previousDistance != distance && DistanceChanged != null)
            {
                DistanceChanged(previousDistance, distance);
            }
        }
    }
}
