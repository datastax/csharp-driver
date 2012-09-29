using System;
using System.Collections.Generic;
using System.Text;
using System.IO;

namespace Cassandra.Native
{
    public partial class CqlRowsPopulator
    {
        public delegate string CellEncoder(string text);

        public void PrintTo(TextWriter stream,
            string delim = "\t|",
            string rowDelim = "\r\n",
            bool printHeader = true,
            bool printFooter = true,
            string separ = "-------------------------------------------------------------------------------",
            string lasLFrm = "Returned {0} rows.",
            CellEncoder cellEncoder = null
            )
        {
            if (printHeader)
            {
                bool first = true;
                foreach (var column in Columns)
                {
                    if (first) first = false;
                    else
                        stream.Write(delim);

                    stream.Write(column.Name);
                }
                stream.Write(rowDelim);
                stream.Write(separ);
                stream.Write(rowDelim);
            }
            int i = 0;
            foreach (var row in GetRows())
            {
                bool first = true;
                for (int j = 0; j < Columns.Length; j++)
                {
                    if (first) first = false;
                    else
                        stream.Write(delim);

                    stream.Write(cellEncoder == null ? row[j] : cellEncoder(row[j].ToString()));
                }
                stream.Write(rowDelim);
                i++;
            }
            if (printFooter)
            {
                stream.Write(separ);
                stream.Write(rowDelim);
                stream.Write(string.Format(lasLFrm, i));
                stream.Write(rowDelim);
            }
        }
    }
}
