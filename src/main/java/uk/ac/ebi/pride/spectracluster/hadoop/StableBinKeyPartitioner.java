package uk.ac.ebi.pride.spectracluster.hadoop;

import org.apache.hadoop.io.*;
import org.systemsbiology.hadoop.*;
import uk.ac.ebi.pride.spectracluster.keys.*;


/**
 * uk.ac.ebi.pride.spectracluster.hadoop.StableChargeBinKeyPartitioner
 * A partitioner which guarantees that given a key representing a StableChargeBinMZKey that
 * all value with a given charge and bin go to the same reducer
 * User: Steve
 * Date: 8/14/13
 */
public class StableBinKeyPartitioner extends AbstractBinnedAPrioriPartitioner {


    @Override
    public int getPartition(final Text pText, final Text value, final int numberReducers) {


        String key = pText.toString();
        // send all special keys to reducer 0
        if(AbstractParameterizedReducer.isKeySpecial(key))
            return 0;
        StableBinMZKey realKey;
        if(key.contains(StableBinMZKey.SORT_PREFIX))
            realKey = new StableBinMZKey(key);
        else {
            realKey = new UnStableBinMZKey(key);
        }
        if(getPrepartitioning(numberReducers) != null)   {
              int preBin = getPrepartitioning(numberReducers).getBin(realKey.getPrecursorMZ());
              if(preBin > -1)
                  return preBin;
          }

        int hash = realKey.getPartitionHash();
        return hash % numberReducers;
     }
}
