import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;

/**
 * Created by zjh on 14-9-29.
 */
public class InitData {
    public static void main(String[] args) throws IOException{
        //创建一个word表，只有一个列族content
        HBaseUtil.createTable("word", "content");
        //获取word表
        HTable htable = HBaseUtil.getHTable("word");
        htable.setAutoFlush(false);

        //创建测试数据
        List<Put> puts = new ArrayList<Put>();

        Put put1 = HBaseUtil.getPut("1","content",null,"The Apache Hadoop software library is a framework");
        Put put2 = HBaseUtil.getPut("2","content",null,"The common utilities that support the other Hadoop modules");
        Put put3 = HBaseUtil.getPut("3","content",null,"Hadoop by reading the documentation");
        Put put4 = HBaseUtil.getPut("4","content",null,"Hadoop from the release page");
        Put put5 = HBaseUtil.getPut("5","content",null,"Hadoop on the mailing list");

        puts.add(put1);
        puts.add(put2);
        puts.add(put3);
        puts.add(put4);
        puts.add(put5);

        //提交测试数据
        htable.put(puts);
        htable.flushCommits();
        htable.close();
        //创建stat表，只有一个列result
        HBaseUtil.createTable("stat","result");
    }
}
