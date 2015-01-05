package uk.ac.ebi.pride.spectracluster.hadoop.io;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.LocalFileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.TaskAttemptID;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.util.ReflectionUtils;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.net.URL;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * @author Rui Wang
 * @version $Id$
 */
public class MGFInputReaderTest {

    private RecordReader<Text, Text> recordReader;

    @Before
    public void setUp() throws Exception {
        Configuration configuration = new Configuration(false);
        configuration.set("fs.default.name", "file:///");
        configuration.set("fs.file.impl", LocalFileSystem.class.getName());

        URL resource = MGFInputReaderTest.class.getClassLoader().getResource("./mgf/sample.mgf");
        File mgfFile = new File(resource.toURI());
        Path path = new Path(mgfFile.getAbsoluteFile().toURI().toString());
        FileSplit split = new FileSplit(path, 0, mgfFile.length(), null);

        MGFInputFormat mgfInputFormat = ReflectionUtils.newInstance(MGFInputFormat.class, configuration);
        TaskAttemptContext taskAttemptContext = new TaskAttemptContext(configuration, new TaskAttemptID());

        recordReader = mgfInputFormat.createRecordReader(split, taskAttemptContext);

        recordReader.initialize(split, taskAttemptContext);
    }

    @Test
    public void testMGFInputReader() throws Exception {

        int count = 0;

        while(recordReader.nextKeyValue()) {
            count ++;

            if (count == 1) {
                Text value = recordReader.getCurrentValue();
                String valueStr = value.toString();
                assertTrue(valueStr.endsWith("583.10156 39.2\nEND IONS\n"));
            }


            if (count == 210) {
                Text value = recordReader.getCurrentValue();
                String valueStr = value.toString();
                assertTrue(valueStr.endsWith("708.46387 15.6\nEND IONS\n"));
            }
        }

        assertEquals(210, count);

    }
}
