package uk.ac.ebi.pride.spectracluster.hadoop.hbase;


import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.*;
import uk.ac.ebi.pride.spectracluster.hadoop.*;

public class SimpleHBaseExample {




    public static void main(String args[]) {
     //   SpectraHadoopUtilities.showSystemProperties();
        try {
             System.out.println(" 0. TRY block1 success !");
        } catch (Exception e) {
            System.out.println("Error on TRY block1" + e.getMessage());
        }

        System.out.println(" 1. From program:Hello from MySimpleHBase");
        System.out.println(" 2. From program:Create a HBase config");
        try {
            HTable table = HBaseUtilities.getTable("t1");

            //noinspection UnnecessaryLocalVariable,UnusedDeclaration,UnusedAssignment
            String s = Bytes.toString(table.getTableName());

            table.close();

            System.out.println(" HBase config object created ");
        } catch (Exception e) {
            System.out.println("Error on HBase block" + e.getMessage());
        }

    }
}