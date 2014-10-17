package uk.ac.ebi.pride.spectracluster;

import java.io.*;
import java.util.*;

/**
 * uk.ac.ebi.pride.spectracluster.FindBadLine
 * User: Steve
 * Date: 9/6/2014
 */
public class FindBadLine {
    public static final String BAD_LINE = "USER02=Uniprot_Z_mays_20091203.fasta:tr|Q8W233|Q8W23";
    public static final int NUMBER_SAVED_LINES = 1000;

    public static class LineHolder {
        private final Deque<String>  held = new ArrayDeque<String>();

        public void add(String s)  {
            held.addLast(s);
            while(held.size() > NUMBER_SAVED_LINES)
                held.removeFirst();
        }

        public void drainTo(Appendable out)   {
            try {
                while(!held.isEmpty())  {
                    out.append(held.removeFirst());
                    out.append("\n");
                }
            }
            catch (IOException e) {
                throw new RuntimeException(e);

            }

        }

    }

    public static void main(String[] args) throws Exception {
        LineHolder lh = new LineHolder();
        File inp = new File(args[0]);
        File outp = new File(args[1]);
        int linesProcessed = 0;

        PrintWriter out = new PrintWriter(new FileWriter(outp));
        LineNumberReader rdr = new LineNumberReader(new FileReader(inp));

        String line = rdr.readLine();
        while(line != null) {
            linesProcessed++;
            lh.add(line);
            if(line.contains(BAD_LINE))
                break;
             line = rdr.readLine();
        }
        line = rdr.readLine();
        lh.drainTo(out);
        int addedLines = 0;
        while(line != null) {
            linesProcessed++;
             if(addedLines++ > NUMBER_SAVED_LINES)
                 break;
            out.append(line);
            out.append("\n");
                line = rdr.readLine();
         }
        out.close();


    }
}
