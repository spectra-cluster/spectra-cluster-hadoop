package uk.ac.ebi.pride.spectracluster.hadoop.bin;

import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.LineNumberReader;
import java.util.ArrayList;
import java.util.List;

/**
 * Reader for parsing binning details from an external resource
 *
 * @author Rui Wang
 * @version $Id$
 */
public final class BinningReader {

    private BinningReader(){}

    public static List<MarkedNumber<String>> read(String realName) throws IOException {
        InputStream resourceAsStream = APrioriBinning.class.getResourceAsStream(realName);
        LineNumberReader rdr = new LineNumberReader(new InputStreamReader(resourceAsStream));

        List<MarkedNumber<String>> holder = new ArrayList<MarkedNumber<String>>();
        String line;
        while ((line = rdr.readLine()) != null) {
            String[] items = line.split("\t");
            if (items.length == 2) {
                MarkedNumber<String> mark = new MarkedNumber<String>(items[0], Double.parseDouble(items[1]));
                holder.add(mark);
            }
        }

        return holder;
    }
}
