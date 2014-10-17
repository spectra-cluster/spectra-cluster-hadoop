package uk.ac.ebi.pride.spectracluster.hadoop.hbase;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Date;
import java.util.NavigableMap;
import java.util.UUID;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.HTablePool;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;

/**
 * Demo implementation of HBase based blog storing and loadin.
 * See http://blog.teamleadnet.com/2013/03/getting-started-with-hbase-in-java.html
 *
 * @author Adam Horvath
 */
public class BlogAPI {
    private static final String blogColumnFamily = "blog";
    private static final String blogsTable = "blogs";
    private static final String usersTable = "users";

    private static Configuration conf;
    private static HTablePool pool;

    // A sample userid. No user API is provided in this demo.
    private static UUID userid =
            UUID.fromString("21d211f0-731c-11e2-bcfd-0800200c9a66");

    static {
        // By default, it's localhost, don't worry.
        conf = HBaseConfiguration.create();
        // Without pooling, the connection to a table will be reinitialized.
        // Creating a new connection to a table might take up to 5-10 seconds!
        pool = new HTablePool(conf, 10);

        // If you don't have tables or column families, HBase will throw an
        // exception. Need to pre-create those. If already exists, it will throw
        // as well. Ah, tricky... :)
        try {
            initDatabase();
        } catch (IOException e) {
        }
    }

    /**
     * Creates the tables and table columns in the database.
     *
     * @throws IOException
     */
    public static void initDatabase() throws IOException {
        HBaseAdmin admin = new HBaseAdmin(conf);
        HTableDescriptor[] blogs = admin.listTables(blogsTable);
        HTableDescriptor[] users = admin.listTables(usersTable);

        if (blogs.length == 0) {
            HTableDescriptor blogstable = new HTableDescriptor(blogsTable);
            admin.createTable(blogstable);
            // Cannot edit a stucture on an active table.
            admin.disableTable(blogsTable);

            HColumnDescriptor blogdesc = new HColumnDescriptor(blogColumnFamily);
            admin.addColumn(blogsTable, blogdesc);

            HColumnDescriptor commentsdesc = new HColumnDescriptor("comments");
            admin.addColumn(blogsTable, commentsdesc);

            // For readin, it needs to be re-enabled.
            admin.enableTable(blogsTable);
        }

        if (users.length == 0) {
            HTableDescriptor blogstable = new HTableDescriptor(usersTable);
            admin.createTable(blogstable);
            admin.disableTable(usersTable);

            HColumnDescriptor userdesc = new HColumnDescriptor("user");
            admin.addColumn(usersTable, userdesc);

            admin.enableTable(usersTable);
        }

        admin.close();
    }

    /**
     * @return List of Blog items.
     * @throws IOException
     */
    public Iterable<Blog> getBlogs() throws IOException {
        HTableInterface table = pool.getTable(blogsTable);

        Scan scan = new Scan();
        scan.addFamily(Bytes.toBytes(blogColumnFamily));

        // For a range scan, set start / stop id or just start.
        // scan.setStartRow(Bytes.toBytes("id11"));
        // scan.setStopRow(Bytes.toBytes("id12"));

        ArrayList<Blog> blogs = new ArrayList<Blog>();

        ResultScanner resultScanner = table.getScanner(scan);
        // For each row
        for (Result result : resultScanner) {
            for (KeyValue kv : result.raw()) {
                Blog b = new Blog();
                b.setTitle(Bytes.toString(kv.getQualifier()));
                b.setBody(Bytes.toString(kv.getValue()));
                b.setId(Bytes.toString(result.getRow()));
                blogs.add(b);
            }
        }

        resultScanner.close();
        table.close();

        return blogs;
    }

    /**
     * Retrieve a single blog post.
     *
     * @param id User ID of the author.
     * @return
     * @throws IOException
     */
    public Blog getBlog(UUID id) throws IOException {
        HTableInterface table = pool.getTable(blogsTable);

        Scan scan = new Scan();
        scan.setStartRow(Bytes.toBytes(id.toString()));
        // Don't pre-fetch more than 1 row.
        scan.setCaching(1);

        Blog blog = null;

        ResultScanner resultScanner = table.getScanner(scan);
        Result result = resultScanner.next();

        // If we want to access the column names as values, this is the 'nicest'
        // way.
        NavigableMap<byte[], byte[]> blogmap =
                result.getFamilyMap(Bytes.toBytes(blogColumnFamily));

        blog = new Blog();
        // The tricky part is that the column key is the title, the column value
        // is the body.
        blog.setTitle(Bytes.toString(blogmap.firstEntry().getKey()));
        blog.setBody(Bytes.toString(blogmap.firstEntry().getKey()));
        blog.setId(Bytes.toString(result.getRow()));

        resultScanner.close();
        table.close();

        return blog;
    }

    /**
     * Store a single blog post with key format of "userid-dattime"
     *
     * @param blog
     * @throws IOException
     */
    public void addBlog(Blog blog) throws IOException {
        HTableInterface table = pool.getTable(blogsTable);
        Put b = new Put(Bytes.toBytes(userid.toString() + new Date().getTime()));
        b.add(
                Bytes.toBytes(blogColumnFamily), // Family ('blog')
                Bytes.toBytes(blog.getTitle()), // Column (the title of the blog post)
                Bytes.toBytes(blog.getBody())); // Value (the body of the blog post).
        table.put(b);
        table.close();
    }

    public static class Blog {
        private   String id;
        private   String title;
        private   String body;

        public Blog() {
        }

        public Blog(String id, String title, String body) {
            this.id = id;
            this.title = title;
            this.body = body;
        }

        public void setId(String id) {
            this.id = id;
        }

        public void setTitle(String title) {
            this.title = title;
        }

        public void setBody(String body) {
            this.body = body;
        }

        public String getId() {
            return id;
        }

        public String getTitle() {
            return title;
        }

        public String getBody() {
            return body;
        }
    }
}
