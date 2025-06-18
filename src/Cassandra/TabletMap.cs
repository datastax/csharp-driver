using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Collections.Immutable;
using System.Linq;
using System.Threading.Tasks;

namespace Cassandra
{
    internal class TabletMap
    {
        private static readonly Logger Logger = new Logger(typeof(TabletMap));
        private static readonly IReadOnlyList<Host> EMPTY_LIST = new List<Host>();

        private readonly ConcurrentDictionary<KeyspaceTableNamePair, TabletSet> _mapping;
        private readonly Metadata _metadata;

        public readonly Action<ITabletMapUpdateRequest> OnTabletMapUpdate;

        public TabletMap(Metadata metadata, ConcurrentDictionary<KeyspaceTableNamePair, TabletSet> mapping, Hosts hosts)
        {
            _metadata = metadata;
            _metadata.SchemaChangedEvent += SchemaCHangedEventHandler;
            _mapping = mapping;
            OnTabletMapUpdate = request => request.Apply(this);
            if (hosts != null)
            {
                hosts.Removed += OnHostRemoved;
            }
        }

        private void SchemaCHangedEventHandler(object sender, SchemaChangedEventArgs e)
        {
            if (e == null)
            {
                return;
            }

            switch (e.What)
            {
                case SchemaChangedEventArgs.Kind.Dropped:
                    if (e.Keyspace != null)
                    {
                        if (e.Table != null)
                        {
                            OnTabletMapUpdate.Invoke(new RemoveAllTabletsForTable(e.Keyspace, e.Table));
                        }
                        else
                        {
                            OnTabletMapUpdate.Invoke(new RemoveAllTabletsForKeyspace(e.Keyspace));
                        }
                    }
                    break;
            }
        }

        private void OnHostRemoved(Host h)
        {
            OnTabletMapUpdate.Invoke(new RemoveAllTabletsForHost(h));
        }

        public static TabletMap EmptyMap(Metadata metadata, Hosts hosts)
        {
            return new TabletMap(metadata, new ConcurrentDictionary<KeyspaceTableNamePair, TabletSet>(), hosts);
        }

        public IDictionary<KeyspaceTableNamePair, TabletSet> GetMapping() => _mapping;

        public IReadOnlyList<Host> GetReplicas(string keyspace, string table, long token)
        {
            var key = new KeyspaceTableNamePair(keyspace, table);

            if (!_mapping.TryGetValue(key, out var tabletSet))
            {
                Logger.Info("No tablets for {keyspace}.{table} in mapping.", keyspace, table);
                return EMPTY_LIST;
            }

            var row = tabletSet.Tablets.FirstOrDefault(t => t.LastToken >= token);
            if (row == null || row.FirstToken >= token)
            {
                Logger.Info("Could not find tablet for {keyspace}.{table} owning token {token}.", keyspace, table, token);
                return EMPTY_LIST;
            }

            var replicas = new List<Host>();
            foreach (var hostShardPair in row.Replicas)
            {
                Host replica = _metadata.Hosts.ToCollection().FirstOrDefault(h => h.HostId == hostShardPair.HostID);
                if (replica == null)
                    return EMPTY_LIST;

                replicas.Add(replica);
            }
            return replicas.ToList(); // Return as List<HostDummy>, which implements IReadOnlyList<HostDummy>
        }

        public void RemoveTableMappings(KeyspaceTableNamePair key) => _mapping.TryRemove(key, out _);

        public void RemoveTableMappings(Host h)
        {
            // TODO: Implement logic to remove all tablets for the specified host.
        }

        public void RemoveTableMappings(string keyspace, string table) => RemoveTableMappings(new KeyspaceTableNamePair(keyspace, table));

        public void RemoveTableMappings(string keyspace)
        {
            foreach (var key in _mapping.Keys.Where(k => k.Keyspace == keyspace).ToList())
            {
                _mapping.TryRemove(key, out _);
            }
        }

        public void AddTablet(string keyspace, string table, Tablet t)
        {
            var ktPair = new KeyspaceTableNamePair(keyspace, table);
            var tabletSet = _mapping.GetOrAdd(ktPair, k => new TabletSet());
            tabletSet.UpdateTablets(currentTablets =>
            {
                // Remove any existing tablets that overlap with the new one
                var overlappingTablets = currentTablets.Where(existingTablet =>
                    existingTablet.FirstToken < t.LastToken && existingTablet.LastToken > t.FirstToken).ToList();
                var updated = currentTablets;
                foreach (var tabletToRemove in overlappingTablets)
                {
                    updated = updated.Remove(tabletToRemove);
                }
                // Add the new tablet
                updated = updated.Add(t);
                return updated;
            });
        }

        public class KeyspaceTableNamePair : IEquatable<KeyspaceTableNamePair>
        {
            public string Keyspace { get; }
            public string TableName { get; }

            public KeyspaceTableNamePair(string keyspace, string tableName)
            {
                Keyspace = keyspace;
                TableName = tableName;
            }

            public override string ToString() => $"KeyspaceTableNamePair{{keyspace='{Keyspace}', tableName='{TableName}'}}";

            public override bool Equals(object obj) => Equals(obj as KeyspaceTableNamePair);
            public bool Equals(KeyspaceTableNamePair other) => other != null && Keyspace == other.Keyspace && TableName == other.TableName;

            public override int GetHashCode() =>
                // Manual hash code implementation for .NET Standard 2.0
                (Keyspace != null ? Keyspace.GetHashCode() : 0) * 397 ^ (TableName != null ? TableName.GetHashCode() : 0);
        }

        public class TabletSet
        {
            private ImmutableSortedSet<Tablet> _tablets = ImmutableSortedSet<Tablet>.Empty;
            public ImmutableSortedSet<Tablet> Tablets => _tablets;

            public void UpdateTablets(Func<ImmutableSortedSet<Tablet>, ImmutableSortedSet<Tablet>> updateFunc)
            {
                ImmutableSortedSet<Tablet> oldSet, newSet;
                do
                {
                    oldSet = _tablets;
                    newSet = updateFunc(oldSet);
                } while (System.Threading.Interlocked.CompareExchange(ref _tablets, newSet, oldSet) != oldSet);
            }
        }
    }
}
