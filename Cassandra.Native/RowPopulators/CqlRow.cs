using System.Collections.Generic;

namespace Cassandra
{
    public class CqlRow
    {
        public readonly object[] Columns;
        readonly Dictionary<string, int> columnIdxes;
        internal CqlRow(OutputRows rawrows, Dictionary<string, int> columnIdxes)
        {
            Columns = new object[rawrows.Metadata.Columns.Length];
            this.columnIdxes = columnIdxes;
            int i = 0;
            foreach (var len in rawrows.GetRawColumnLengths())
            {
                if (len < 0)
                    Columns[i] = null;
                else
                {
                    byte[] buffer = new byte[len];

                    rawrows.ReadRawColumnValue(buffer, 0, len);
                    Columns[i] = TypeInterpreter.CqlConvert(buffer,
                        rawrows.Metadata.Columns[i].TypeCode, rawrows.Metadata.Columns[i].TypeInfo);                    
                }

                i++;
                if (i >= rawrows.Metadata.Columns.Length)
                    break;                                
            }
        }

        public int Length
        {
            get
            {
                return Columns.Length;
            }
        }

        public object this[int idx]
        {
            get
            {
                return Columns[idx];
            }
        }

        public object this[string name]
        {
            get
            {
                return Columns[columnIdxes[name]];
            }
        }

        public bool IsNull(string name)
        {
            return this[name] == null;
        }

        public bool IsNull(int idx)
        {
            return this[idx] == null;
        }

        public T GetValue<T>(string name)
        {
            return (T)this[name];
        }

        public T GetValue<T>(int idx)
        {
            return (T)this[idx];
        }
    }
}
