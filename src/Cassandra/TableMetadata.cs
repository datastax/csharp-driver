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
﻿using System.Collections.Generic;
using System;
using System.Text;

namespace Cassandra
{
    public enum KeyType
    {
        None = 0,
        Partition = 1,
        Clustering = 2,
        SecondaryIndex = 3
    }

    public class TableColumn : CqlColumn
    {
        public KeyType KeyType;
        public string SecondaryIndexName;
        public string SecondaryIndexType;
    }

    public class TableMetadata
    {

        public string Name { get; private set; }


        public TableColumn[] TableColumns { get; private set; }

        public TableOptions Options{ get; private set; }

        internal TableMetadata(string name, TableColumn[] tableColumns, TableOptions options)
        {
            Name = name;
            TableColumns = tableColumns;
            Options = options;
        }

        internal TableMetadata(BEBinaryReader reader)
        {
            List<TableColumn> coldat = new List<TableColumn>();
            var flags = (FlagBits)reader.ReadInt32();
            var numberOfcolumns = reader.ReadInt32();
            this.TableColumns = new TableColumn[numberOfcolumns];
            string gKsname = null;
            string gTablename = null;

            if ((flags & FlagBits.GlobalTablesSpec) == FlagBits.GlobalTablesSpec)
            {
                gKsname = reader.ReadString();
                gTablename = reader.ReadString();
            }
            for (int i = 0; i < numberOfcolumns; i++)
            {
                var col = new TableColumn();
                if ((flags & FlagBits.GlobalTablesSpec) != FlagBits.GlobalTablesSpec)
                {
                    col.Keyspace = reader.ReadString();
                    col.Table = reader.ReadString();
                }
                else
                {
                    col.Keyspace = gKsname;
                    col.Table = gTablename;
                }
                col.Name = reader.ReadString();
                col.TypeCode = (ColumnTypeCode)reader.ReadUInt16();
                col.TypeInfo = GetColumnInfo(reader, col.TypeCode);
                coldat.Add(col);
            }
            TableColumns = coldat.ToArray();
        }

        private IColumnInfo GetColumnInfo(BEBinaryReader reader, ColumnTypeCode code)
        {
            ColumnTypeCode innercode;
            ColumnTypeCode vinnercode;
            switch (code)
            {
                case ColumnTypeCode.Custom:
                    return new CustomColumnInfo() { CustomTypeName = reader.ReadString() };
                case ColumnTypeCode.List:
                    innercode = (ColumnTypeCode)reader.ReadUInt16();
                    return new ListColumnInfo()
                    {
                        ValueTypeCode = innercode,
                        ValueTypeInfo = GetColumnInfo(reader, innercode)
                    };
                case ColumnTypeCode.Map:
                    innercode = (ColumnTypeCode)reader.ReadUInt16();
                    var kci = GetColumnInfo(reader, innercode);
                    vinnercode = (ColumnTypeCode)reader.ReadUInt16();
                    var vci = GetColumnInfo(reader, vinnercode);
                    return new MapColumnInfo()
                    {
                        KeyTypeCode = innercode,
                        KeyTypeInfo = kci,
                        ValueTypeCode = vinnercode,
                        ValueTypeInfo = vci
                    };
                case ColumnTypeCode.Set:
                    innercode = (ColumnTypeCode)reader.ReadUInt16();
                    return new SetColumnInfo()
                    {
                        KeyTypeCode = innercode,
                        KeyTypeInfo = GetColumnInfo(reader, innercode)
                    };
                default:
                    return null;
            }
        }
    }
	
    public class TableOptions 
    {
        private string COMMENT = "comment";
        private string READ_REPAIR = "read_repair_chance";
        private string LOCAL_READ_REPAIR = "dclocal_read_repair_chance";
        private string REPLICATE_ON_WRITE = "replicate_on_write";
        private string GC_GRACE = "gc_grace_seconds";
        private string BF_FP_CHANCE = "bloom_filter_fp_chance";
        private string CACHING = "caching";
        private string COMPACTION_OPTIONS = "compaction";
        private string COMPRESSION_PARAMS = "compression";

        internal bool isCompactStorage; 
        internal string comment;
        internal double readRepair;
        internal double localReadRepair;
        internal bool replicateOnWrite;
        internal int gcGrace;
        internal double bfFpChance;
        internal string caching;
        internal SortedDictionary<string, string> compactionOptions;
        internal SortedDictionary<string, string> compressionParams;

        /// <summary>
        ///  Whether the table uses the <code>COMPACT STORAGE</code> option.
        /// </summary>
        /// 
        /// <returns>whether the table uses the <code>COMPACT STORAGE</code>
        ///  option.</returns>
        public bool IsCompactStorage { get { return isCompactStorage; } }
        /// <summary>
        ///  The commentary set for this table.
        /// </summary>
        /// 
        /// <returns>the commentary set for this table, or <code>null</code> if noe has
        ///  been set.</returns>
        public string Comment { get { return comment; } }
        /// <summary>
        ///  The chance with which a read repair is triggered for this table.
        /// </summary>
        /// 
        /// <returns>the read repair change set for table (in [0.0, 1.0]).</returns>
        public double ReadRepairChance { get { return readRepair; } }
        /// <summary>
        ///  The (cluster) local read repair chance set for this table.
        /// </summary>
        /// 
        /// <returns>the local read repair change set for table (in [0.0, 1.0]).</returns>
        public double LocalReadRepairChance { get { return localReadRepair; } }
        /// <summary>
        ///  Whether replicateOnWrite is set for this table. This is only meaningful for
        ///  tables holding counters.
        /// </summary>
        /// 
        /// <returns>whether replicateOnWrite is set for this table.</returns>
        public bool ReplicateOnWrite { get { return replicateOnWrite; } }
        /// <summary>
        ///  The tombstone garbage collection grace time in seconds for this table.
        /// </summary>
        /// 
        /// <returns>the tombstone garbage collection grace time in seconds for this
        ///  table.</returns>
        public int GcGraceSeconds { get { return gcGrace; } }
        /// <summary>
        ///  The false positive chance for the bloom filter of this table.
        /// </summary>
        /// 
        /// <returns>the bloom filter false positive chance for this table (in [0.0,
        ///  1.0]).</returns>
        public double BloomFilterFpChance { get { return bfFpChance; } }
        /// <summary>
        ///  The caching option for this table.
        /// </summary>
        /// 
        /// <returns>the caching option for this table.</returns>
        public string Caching { get { return caching; } }
        /// <summary>
        ///  The compaction options for this table.
        /// </summary>
        /// 
        /// <returns>a dictionary containing the compaction options for this table.</returns>
        public SortedDictionary<string, string> CompactionOptions { get { return compactionOptions; } }
        /// <summary>
        ///  The compression options for this table.
        /// </summary>
        /// 
        /// <returns>a dictionary containing the compression options for this table.</returns>
        public SortedDictionary<string, string> CompressionParams { get { return compressionParams; } }

        public TableOptions() { }
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
            Dictionary<string, string> opt = 
                new Dictionary<string, string>() {
                                                    {COMMENT,comment},
                                                    {READ_REPAIR , readRepair.ToString()},
                                                    {LOCAL_READ_REPAIR , localReadRepair.ToString()},
                                                    {REPLICATE_ON_WRITE , replicateOnWrite.ToString()},
                                                    {GC_GRACE , gcGrace.ToString()},
                                                    {BF_FP_CHANCE , bfFpChance.ToString()},
                                                    {CACHING , caching},
                                                    {COMPACTION_OPTIONS , Utils.ConvertToCqlMap(compactionOptions)},
                                                    {COMPRESSION_PARAMS , Utils.ConvertToCqlMap(compressionParams)}};
                                                                                                                                                                      
            StringBuilder sb = new StringBuilder();
            bool first = true;
            foreach (var ent in opt)
            {
                if(ent.Value.Contains("{"))
                    sb.Append((first ? "" : " AND ") + ent.Key + " = " + ent.Value);
                else
                    sb.Append((first ? "" : " AND ") + ent.Key + " = '" + ent.Value + "'");
                first = false;
            }
            return sb.ToString();
        }

        public override bool Equals(object obj)
        {
            TableOptions tabOpt = obj as TableOptions;
            if (tabOpt == null)
                return false;
            
            return 
                this.bfFpChance == tabOpt.bfFpChance &&
                this.caching == tabOpt.caching &&
                this.comment == tabOpt.comment &&                
                Utils.CompareIDictionary(this.compactionOptions , tabOpt.compactionOptions) &&
                Utils.CompareIDictionary(this.compressionParams, tabOpt.compressionParams) &&
                this.gcGrace == tabOpt.gcGrace &&
                this.isCompactStorage == tabOpt.isCompactStorage &&
                this.localReadRepair == tabOpt.localReadRepair &&
                this.readRepair == tabOpt.readRepair &&
                this.replicateOnWrite == tabOpt.replicateOnWrite;                            
        }

        public override int GetHashCode()
        {
            return base.GetHashCode();
        }
    }
}
