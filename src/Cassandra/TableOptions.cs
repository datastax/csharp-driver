using System.Collections.Generic;
using System.Globalization;
using System.Text;

namespace Cassandra
{
    public class TableOptions
    {
        private string BF_FP_CHANCE = "bloom_filter_fp_chance";
        private string CACHING = "caching";
        private string COMMENT = "comment";
        private string COMPACTION_OPTIONS = "compaction";
        private string COMPRESSION_PARAMS = "compression";
        private string GC_GRACE = "gc_grace_seconds";
        private string LOCAL_READ_REPAIR = "dclocal_read_repair_chance";
        private string READ_REPAIR = "read_repair_chance";
        private string REPLICATE_ON_WRITE = "replicate_on_write";

        internal double bfFpChance;
        internal string caching;
        internal string comment;
        internal SortedDictionary<string, string> compactionOptions;
        internal SortedDictionary<string, string> compressionParams;
        internal int gcGrace;
        internal bool isCompactStorage;
        internal double localReadRepair;
        internal double readRepair;
        internal bool replicateOnWrite;

        /// <summary>
        ///  Whether the table uses the <c>COMPACT STORAGE</c> option.
        /// </summary>
        /// 
        /// <returns>whether the table uses the <c>COMPACT STORAGE</c>
        ///  option.</returns>
        public bool IsCompactStorage
        {
            get { return isCompactStorage; }
        }

        /// <summary>
        ///  The commentary set for this table.
        /// </summary>
        /// 
        /// <returns>the commentary set for this table, or <c>null</c> if noe has
        ///  been set.</returns>
        public string Comment
        {
            get { return comment; }
        }

        /// <summary>
        ///  The chance with which a read repair is triggered for this table.
        /// </summary>
        /// 
        /// <returns>the read repair change set for table (in [0.0, 1.0]).</returns>
        public double ReadRepairChance
        {
            get { return readRepair; }
        }

        /// <summary>
        ///  The (cluster) local read repair chance set for this table.
        /// </summary>
        /// 
        /// <returns>the local read repair change set for table (in [0.0, 1.0]).</returns>
        public double LocalReadRepairChance
        {
            get { return localReadRepair; }
        }

        /// <summary>
        ///  Whether replicateOnWrite is set for this table. This is only meaningful for
        ///  tables holding counters.
        /// </summary>
        /// 
        /// <returns>whether replicateOnWrite is set for this table.</returns>
        public bool ReplicateOnWrite
        {
            get { return replicateOnWrite; }
        }

        /// <summary>
        ///  The tombstone garbage collection grace time in seconds for this table.
        /// </summary>
        /// 
        /// <returns>the tombstone garbage collection grace time in seconds for this
        ///  table.</returns>
        public int GcGraceSeconds
        {
            get { return gcGrace; }
        }

        /// <summary>
        ///  The false positive chance for the bloom filter of this table.
        /// </summary>
        /// 
        /// <returns>the bloom filter false positive chance for this table (in [0.0,
        ///  1.0]).</returns>
        public double BloomFilterFpChance
        {
            get { return bfFpChance; }
        }

        /// <summary>
        ///  The caching option for this table.
        /// </summary>
        /// 
        /// <returns>the caching option for this table.</returns>
        public string Caching
        {
            get { return caching; }
        }

        /// <summary>
        ///  The compaction options for this table.
        /// </summary>
        /// 
        /// <returns>a dictionary containing the compaction options for this table.</returns>
        public SortedDictionary<string, string> CompactionOptions
        {
            get { return compactionOptions; }
        }

        /// <summary>
        ///  The compression options for this table.
        /// </summary>
        /// 
        /// <returns>a dictionary containing the compression options for this table.</returns>
        public SortedDictionary<string, string> CompressionParams
        {
            get { return compressionParams; }
        }

        public TableOptions()
        {
        }

        public TableOptions(string comment, double readRepair, double localReadRepair, bool replicateOnWrite, int gcGrace, double bfFpChance,
                            string caching, SortedDictionary<string, string> compactionOptions, SortedDictionary<string, string> compressionParams)
        {
            this.comment = comment;
            this.readRepair = readRepair;
            this.localReadRepair = localReadRepair;
            this.replicateOnWrite = replicateOnWrite;
            this.gcGrace = gcGrace;
            this.bfFpChance = bfFpChance;
            this.caching = caching;
            this.compactionOptions = compactionOptions;
            this.compressionParams = compressionParams;
        }

        public override string ToString()
        {
            var opt =
                new Dictionary<string, string>
                {
                    {COMMENT, comment},
                    {READ_REPAIR, readRepair.ToString(CultureInfo.InvariantCulture)},
                    {LOCAL_READ_REPAIR, localReadRepair.ToString(CultureInfo.InvariantCulture)},
                    {REPLICATE_ON_WRITE, replicateOnWrite.ToString()},
                    {GC_GRACE, gcGrace.ToString(CultureInfo.InvariantCulture)},
                    {BF_FP_CHANCE, bfFpChance.ToString(CultureInfo.InvariantCulture)},
                    {CACHING, caching},
                    {COMPACTION_OPTIONS, Utils.ConvertToCqlMap(compactionOptions)},
                    {COMPRESSION_PARAMS, Utils.ConvertToCqlMap(compressionParams)}
                };

            var sb = new StringBuilder();
            bool first = true;
            foreach (KeyValuePair<string, string> ent in opt)
            {
                if (ent.Value.Contains("{"))
                    sb.Append((first ? "" : " AND ") + ent.Key + " = " + ent.Value);
                else
                    sb.Append((first ? "" : " AND ") + ent.Key + " = '" + ent.Value + "'");
                first = false;
            }
            return sb.ToString();
        }

        public override bool Equals(object obj)
        {
            var tabOpt = obj as TableOptions;
            if (tabOpt == null)
                return false;

            return
                bfFpChance == tabOpt.bfFpChance &&
                caching == tabOpt.caching &&
                comment == tabOpt.comment &&
                Utils.CompareIDictionary(compactionOptions, tabOpt.compactionOptions) &&
                Utils.CompareIDictionary(compressionParams, tabOpt.compressionParams) &&
                gcGrace == tabOpt.gcGrace &&
                isCompactStorage == tabOpt.isCompactStorage &&
                localReadRepair == tabOpt.localReadRepair &&
                readRepair == tabOpt.readRepair &&
                replicateOnWrite == tabOpt.replicateOnWrite;
        }

        public override int GetHashCode()
        {
            return base.GetHashCode();
        }
    }
}