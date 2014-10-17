package uk.ac.ebi.pride.spectracluster.hadoop.hbase;
import java.io.IOException;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HTable;
//import org.apache.hadoop.hbase.client.Scanner;
//import org.apache.hadoop.hbase.io.BatchUpdate;
//import org.apache.hadoop.hbase.io.Cell;
//import org.apache.hadoop.hbase.io.RowResult;
//import org.apache.hadoop.hbase.util.Bytes;
//
//public class HBaseExample {
//
//    public static void main(String args[]) throws IOException {
//
//        HBaseConfiguration conf = new HBaseConfiguration();
//        conf.addResource(new Path("/opt/hbase-0.19.3/conf/hbase-site.xml"));
//
//        HTable table = new HTable(conf, "test_table");
//
//        BatchUpdate batchUpdate = new BatchUpdate("test_row1");
//        batchUpdate.put("columnfamily1:column1", Bytes.toBytes("some value"));
//        batchUpdate.delete("column1");
//        table.commit(batchUpdate);
//
//        Cell cell = table.get("test_row1", "columnfamily1:column1");
//        if (cell != null) {
//            String valueStr = Bytes.toString(cell.getValue());
//            System.out.println("test_row1:columnfamily1:column1 " + valueStr);
//        }
//
//        RowResult singleRow = table.getRow(Bytes.toBytes("test_row1"));
//        Cell cell = singleRow.get(Bytes.toBytes("columnfamily1:column1"));
//        if(cell!=null) {
//            System.out.println(Bytes.toString(cell.getValue()));
//        }
//
//        cell = singleRow.get(Bytes.toBytes("columnfamily1:column2"));
//        if(cell!=null) {
//            System.out.println(Bytes.toString(cell.getValue()));
//        }
//
//        Scanner scanner = table.getScanner(
//            new String[] { "columnfamily1:column1" });
//
//        //First approach to iterate a scanner
//        RowResult rowResult = scanner.next();
//        while (rowResult != null) {
//            System.out.println("Found row: " + Bytes.toString(rowResult.getRow())
//                + " with value: " +
//                rowResult.get(Bytes.toBytes("columnfamily1:column1")));
//            rowResult = scanner.next();
//        }
//
//        // The other approach is to use a foreach loop. Scanners are iterable!
//        for (RowResult result : scanner) {
//            // print out the row we found and the columns we were looking for
//            System.out.println("Found row: " + Bytes.toString(result.getRow())
//                + " with value: " +
//                result.get(Bytes.toBytes("columnfamily1:column1")));
//        }
//
//        scanner.close();
//        table.close();
//    }
//}