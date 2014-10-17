package uk.ac.ebi.pride.spectracluster.hadoop;

import org.apache.hadoop.conf.*;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.mapreduce.*;
import org.systemsbiology.hadoop.*;

import java.io.*;
import java.util.*;

/**
 * uk.ac.ebi.pride.spectracluster.hadoop.DaltonBinSize
 *
 * @author Steve Lewis
 * @date 30/10/13
 */
@SuppressWarnings("UnusedDeclaration")
public class DaltonBinSize {

    private static int gTotalSpectra;
    private static Map<Integer, Integer> gMZ_to_number = new HashMap<Integer, Integer>();

    public static final String TSV_COUNTERS_FILE = "DaltonBinCounters.tsv";

    @SuppressWarnings("UnusedDeclaration")
    public static int getNumberSpectraWithMZ(int mz, TaskAttemptContext context) {
        Configuration conf = context.getConfiguration();
        return getNumberSpectraWithMZ(mz, conf);
    }

    public static int getTotalSpectra(Configuration conf) {
        if (gTotalSpectra == 0) {
            populateMap(conf);
        }
        return gTotalSpectra;
    }

    public static int getNumberSpectraWithMZ(int mz, Configuration conf) {
        getTotalSpectra(conf); // this forces reading of the map
        Integer ret = gMZ_to_number.get(mz);
        if (ret == null) {
            gMZ_to_number.put(mz, 0);
            return 0;
        }
        return ret;
    }


    protected static void populateMap(Configuration conf) {
        try {
            FileSystem fs = FileSystem.get(conf);
            if (populateMapFromTSV(conf, fs))
                return;
            populateMapFromCounters(conf, fs);
            writeMapToTSV(conf, fs);
        } catch (IOException e) {
            throw new UnsupportedOperationException(e);
        }

    }

    protected static void writeMapToTSV(Configuration conf, FileSystem fs) {
//        String fileName = HadoopUtilities.buildCounterFileName(TSV_COUNTERS_FILE, conf);
//        Path p = new Path(fileName);
//        Integer[] sortedBins = gMZ_to_number.keySet().toArray(new Integer[gMZ_to_number.keySet().size()]);
//        Arrays.sort(sortedBins);
//        try {
//            FSDataOutputStream fsout = fs.create(p, true);
//            PrintWriter out = new PrintWriter(new OutputStreamWriter(fsout));
//            out.println(HadoopUtilities.TSV_BIN_COUNT_HEADER);
//            //noinspection ForLoopReplaceableByForEach
//            for (int i = 0; i < sortedBins.length; i++) {
//                Integer sortedBin = sortedBins[i];
//                out.println(sortedBin.toString() + "\t" + gMZ_to_number.get(sortedBin));
//            }
//            out.close();
//        } catch (IOException e) {
//            throw new UnsupportedOperationException(e);
//        }
        throw new UnsupportedOperationException("Unimplemented for now"); // ToDo
    }


    protected static boolean populateMapFromTSV(Configuration conf, FileSystem fileSystem) {
//        String fileName = HadoopUtilities.buildCounterFileName(TSV_COUNTERS_FILE, conf);
//        Path p = new Path(fileName);
//        try {
//            if (!fileSystem.exists(p))
//                return false;
//
//            Map<Integer, Integer> ret = HadoopUtilities.readBinCountersFromTSV(fileSystem, fileName);
//            for (Integer key : ret.keySet()) {
//                gTotalSpectra += ret.get(key);
//            }
//            gMZ_to_number.putAll(ret);
//            return true;
//        } catch (IOException e) {
//            throw new UnsupportedOperationException(e);
//        }
         throw new UnsupportedOperationException("Unimplemented Fix This"); // ToDo
    }

    protected static boolean populateMapFromCounters(Configuration conf, FileSystem fileSystem) {
        String fileName = HadoopUtilities.buildCounterFileName("SpectraPeakClustererPass1.counters", conf);
        Path p = new Path(fileName);
        try {
            if (!fileSystem.exists(p))
                return false;

            Map<Integer, Integer> ret = HadoopUtilities.readBinCounters(fileSystem, fileName);
            for (Integer key : ret.keySet()) {
                gTotalSpectra += ret.get(key);
            }
            gMZ_to_number.putAll(ret);
            return true;
        } catch (IOException e) {
            throw new UnsupportedOperationException(e);
        }
    }
}
