using System;
using System.Collections.Generic;
using System.Text;
using System.Net;
using Cassandra.Native;
using System.Threading;
using System.Data.Common;
namespace Cassandra
{
    /**
     * Describes a keyspace defined in this cluster.
     */
    public class KeyspaceMetadata
    {

        public static readonly string KS_NAME = "keyspace_name";
        private static readonly string DURABLE_WRITES = "durable_writes";
        private static readonly string STRATEGY_CLASS = "strategy_class";
        private static readonly string STRATEGY_OPTIONS = "strategy_options";

        private readonly string name;
        private readonly bool durableWrites;
        private readonly Dictionary<string, string> replication = new Dictionary<string, string>();
        //private readonly Dictionary<string, TableMetadata> tables = new Dictionary<string, TableMetadata>();

        private KeyspaceMetadata(string name, bool durableWrites)
        {
            this.name = name;
            this.durableWrites = durableWrites;
        }

        //    static KeyspaceMetadata build(Row row) {

        //        string name = row.getstring(KS_NAME);
        //        bool durableWrites = row.getBool(DURABLE_WRITES);
        //        KeyspaceMetadata ksm = new KeyspaceMetadata(name, durableWrites);
        //        ksm.replication.put("class", row.getstring(STRATEGY_CLASS));
        //        ksm.replication.putAll(TableMetadata.fromJsonMap(row.getstring(STRATEGY_OPTIONS)));
        //        return ksm;
        //    }

        //    /**
        //     * Returns the name of this keyspace.
        //     *
        //     * @return the name of this CQL keyspace.
        //     */
        //    public string getName() {
        //        return name;
        //    }

        //    /**
        //     * Returns whether durable writes are set on this keyspace.
        //     *
        //     * @return {@code true} if durable writes are set on this keyspace (the
        //     * default), {@code false} otherwise.
        //     */
        //    public bool isDurableWrites() {
        //        return durableWrites;
        //    }

        //    /**
        //     * Returns the replication options for this keyspace.
        //     *
        //     * @return a map containing the replication options for this keyspace.
        //     */
        //    public Map<string, string> getReplication() {
        //        return Collections.<string, string>unmodifiableMap(replication);
        //    }

        //    /**
        //     * Returns the metadata for a table contained in this keyspace.
        //     *
        //     * @param name the name of table to retrieve
        //     * @return the metadata for table {@code name} in this keyspace if it
        //     * exists, {@code false} otherwise.
        //     */
        //    public TableMetadata getTable(string name) {
        //        return tables.get(name);
        //    }

        //    /**
        //     * Returns the tables defined in this keyspace.
        //     *
        //     * @return a collection of the metadata for the tables defined in this
        //     * keyspace.
        //     */
        //    public Collection<TableMetadata> getTables() {
        //        return Collections.<TableMetadata>unmodifiableCollection(tables.values());
        //    }

        //    /**
        //     * Return a {@code string} containing CQL queries representing this
        //     * keyspace and the table it contains.
        //     *
        //     * In other words, this method returns the queries that would allow to
        //     * recreate the schema of this keyspace, along with all its table.
        //     *
        //     * Note that the returned string is formatted to be human readable (for
        //     * some defintion of human readable at least).
        //     *
        //     * @return the CQL queries representing this keyspace schema as a {code
        //     * string}.
        //     */
        //    public string exportAsstring() {
        //        stringBuilder sb = new stringBuilder();

        //        sb.append(asCQLQuery()).append("\n");

        //        for (TableMetadata tm : tables.values())
        //            sb.append("\n").append(tm.exportAsstring()).append("\n");

        //        return sb.tostring();
        //    }

        //    /**
        //     * Returns a CQL query representing this keyspace.
        //     *
        //     * This method returns a single 'CREATE KEYSPACE' query with the options
        //     * corresponding to this keyspace definition.
        //     *
        //     * @return the 'CREATE KEYSPACE' query corresponding to this keyspace.
        //     * @see #exportAsstring
        //     */
        //    public string asCQLQuery() {
        //        stringBuilder sb = new stringBuilder();

        //        sb.append("CREATE KEYSPACE ").append(name).append(" WITH ");
        //        sb.append("REPLICATION = { 'class' : '").append(replication.get("class")).append("'");
        //        for (Map.Entry<string, string> entry : replication.entrySet()) {
        //            if (entry.getKey().equals("class"))
        //                continue;
        //            sb.append(", '").append(entry.getKey()).append("': '").append(entry.getValue()).append("'");
        //        }
        //        sb.append(" } AND DURABLE_WRITES = ").append(durableWrites);
        //        sb.append(";");
        //        return sb.tostring();
        //    }

        //    void add(TableMetadata tm) {
        //        tables.put(tm.getName(), tm);
        //    }
    }
}