using System;
using System.Collections.Generic;
using System.Text;

namespace Cassandra.Native
{
    public class CqlRow
    {
        object[] columns;
        Dictionary<string, int> columnIdxes;
        internal CqlRow(OutputRows rawrows, Dictionary<string, int> columnIdxes)
        {
            columns = new object[rawrows.Metadata.Columns.Length];
            this.columnIdxes = columnIdxes;
            int i = 0;
            foreach (var len in rawrows.GetRawColumnLengths())
            {
                if (len < 0)
                    columns[i] = null;
                else
                {
                    byte[] buffer = new byte[len];

                    rawrows.ReadRawColumnValue(buffer, 0, len);
                    columns[i] = TypeInerpreter.CqlConvert(buffer,
                        rawrows.Metadata.Columns[i].type_code, rawrows.Metadata.Columns[i].type_info);
                    i++;
                    if (i >= rawrows.Metadata.Columns.Length)
                        break;
                }
            }
        }

        public int Length
        {
            get
            {
                return columns.Length;
            }
        }

        public object this[int idx]
        {
            get
            {
                return columns[idx];
            }
        }

        public object this[string name]
        {
            get
            {
                return columns[columnIdxes[name]];
            }
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
