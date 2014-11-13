package uk.ac.ebi.pride.spectracluster.hadoop.io;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionCodecFactory;
import org.apache.hadoop.io.compress.CompressionInputStream;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.util.LineReader;

import java.io.IOException;

/**
 * Splitter that reads mgf files
 * nice enough to put the begin and end tags on separate lines
 *
 * @author Steve Lewis
 * @author Rui Wang
 *
 * todo: to be reviewed
 */
public class MGFInputFormat extends FileInputFormat<Text, Text> {

    private String m_Extension = "mgf";

    public MGFInputFormat() {

    }

    public String getExtension() {
        return m_Extension;
    }

    public void setExtension(final String pExtension) {
        m_Extension = pExtension;
    }


    @Override
    public RecordReader<Text, Text> createRecordReader(InputSplit split,
                                                       TaskAttemptContext context) {
        return new MGFFileReader();
    }

    @Override
    protected boolean isSplitable(JobContext context, Path file) {
        final String lcName = file.getName().toLowerCase();
        if (lcName.endsWith("gz"))
            return false;
        return true;
    }

    /**
     * Custom RecordReader which returns the entire file as a
     * single value with the name as a key
     * Value is the entire file
     * Key is the file name
     */
    public class MGFFileReader extends RecordReader<Text, Text> {

        private CompressionCodecFactory compressionCodecs = null;
        private long m_Start;
        private long m_End;
        private long current;
        private LineReader m_Input;
        FSDataInputStream m_RealFile;
        private Text key = null;
        private Text value = null;
        private Text buffer = new Text();

        public void initialize(InputSplit genericSplit,
                               TaskAttemptContext context) throws IOException {
            FileSplit split = (FileSplit) genericSplit;
            Configuration job = context.getConfiguration();
            m_Start = split.getStart();
            m_End = m_Start + split.getLength();
            final Path file = split.getPath();
            compressionCodecs = new CompressionCodecFactory(job);
            boolean skipFirstLine = false;
            final CompressionCodec codec = compressionCodecs.getCodec(file);

            // open the file and seek to the m_Start of the split
            FileSystem fs = file.getFileSystem(job);
            // open the file and seek to the m_Start of the split
            m_RealFile = fs.open(split.getPath());
            if (codec != null) {
                CompressionInputStream inputStream = codec.createInputStream(m_RealFile);
                m_Input = new LineReader( inputStream );
                m_End = Long.MAX_VALUE;
            }
            else {
                if (m_Start != 0) {
                    skipFirstLine = true;
                    --m_Start;
                    m_RealFile.seek(m_Start);
                }
                m_Input = new LineReader( m_RealFile);
            }
            // not at the beginning so go to first line
            if (skipFirstLine) {  // skip first line and re-establish "m_Start".
                m_Start += m_Input.readLine(buffer) ;
            }

            current = m_Start;
            if (key == null) {
                key = new Text();
            }
            key.set(split.getPath().getName());
            if (value == null) {
                value = new Text();
            }

            current = 0;
        }

        /**
         * look for a <scan tag then read until it closes
         *
         * @return true if there is data
         * @throws java.io.IOException
         */
        public boolean nextKeyValue() throws IOException


        {
            int newSize = 0;
            while (current < m_Start) {
                newSize = m_Input.readLine(buffer);
                // we are done
                if (newSize == 0) {
                    key = null;
                    value = null;
                    return false;
                }
                current = m_RealFile.getPos();
            }
            StringBuilder sb = new StringBuilder();
            newSize = m_Input.readLine(buffer);
            String str = null;
            while (newSize > 0) {
                str = buffer.toString();

                if ("BEGIN IONS".equals(str)) {
                    break;
                }
                current = m_RealFile.getPos();
                // we are done
                if (current > m_End) {
                    key = null;
                    value = null;
                    return false;

                }
                newSize = m_Input.readLine(buffer);
            }
            if (newSize == 0) {
                key = null;
                value = null;
                return false;

            }
            while (newSize > 0) {
                str = buffer.toString();
                sb.append(str);
                sb.append("\n");
                if ("END IONS".equals(str)) {
                    break;
                }
                newSize = m_Input.readLine(buffer);
            }

            String s = sb.toString();
            value.set(s);

            if (sb.length() == 0) {
                key = null;
                value = null;
                return false;
            }
            else {
                return true;
            }
        }

        @Override
        public Text getCurrentKey() {
            return key;
        }

        @Override
        public Text getCurrentValue() {
            return value;
        }


        /**
         * Get the progress within the split
         */
        public float getProgress() {
            long totalBytes = m_End - m_Start;
            long totalHandled = current - m_Start;
            return ((float) totalHandled) / totalBytes;
        }


        public synchronized void close() throws IOException {
            if (m_Input != null) {
                m_Input.close();
            }
        }
    }
}
