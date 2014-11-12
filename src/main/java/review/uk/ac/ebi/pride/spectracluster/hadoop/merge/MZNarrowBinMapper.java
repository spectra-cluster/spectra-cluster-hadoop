package review.uk.ac.ebi.pride.spectracluster.hadoop.merge;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import review.uk.ac.ebi.pride.spectracluster.hadoop.util.HadoopDefaults;
import uk.ac.ebi.pride.spectracluster.util.binner.IWideBinner;

import java.io.IOException;

/**
 * Mapper using narrow bins
 *
 * @author Steve Lewis
 * @author Rui Wang
 * @version $Id$
 */
public class MZNarrowBinMapper extends Mapper<Text, Text, Text, Text> {

    private IWideBinner mapperBinner;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        super.setup(context);
        setMapperBinner(HadoopDefaults.DEFAULT_WIDE_MZ_BINNER);
    }

    @Override
    protected void map(Text key, Text value, Context context) throws IOException, InterruptedException {
        super.map(key, value, context);
    }

    public IWideBinner getMapperBinner() {
        return mapperBinner;
    }

    public void setMapperBinner(IWideBinner mapperBinner) {
        this.mapperBinner = mapperBinner;
    }
}
