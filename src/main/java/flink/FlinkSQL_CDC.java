package flink;


import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;


public class FlinkSQL_CDC {

    public static void main(String[] args) throws Exception {

//
//        Configuration conf = new Configuration();
//        conf.setInteger("rest.port",3335);
//        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(conf);

        //1.创建执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);


        //2.创建Flink-MySQL-CDC的Source
        TableResult tableResult = tableEnv.executeSql("CREATE TABLE table_name (" +
                "  id INT primary key," +
                "  name STRING" +
                ") WITH (" +
                "  'connector' = 'mysql-cdc'," +
                "  'hostname' = 'hadoop102'," +
                "  'port' = '3306'," +
                "  'username' = 'root'," +
                "  'password' = 'mivbAs7Awc'," +
                "  'database-name' = 'student'," +
                "  'table-name' = 'table_name'," +
                "'server-time-zone' = 'Asia/Shanghai'," +
                "'scan.startup.mode' = 'initial'" +
                ")"
        );

        // 2. 注册SinkTable: sink_sensor
//        tableEnv.executeSql("" +
//                "CREATE TABLE kafka_binlog ( " +
//                "  user_id INT, " +
//                "  user_name STRING, " +
//                "`proc_time` as PROCTIME()" +
//                ") WITH ( " +
//                "  'connector' = 'kafka', " +
//                "  'topic' = 'test2', " +
//                "  'properties.bootstrap.servers' = 'hadoop102:9092', " +
//                "  'format' = 'json' " +
//                ")" +
//                "");

        //upsert-kafka
        tableEnv.executeSql("" +
                "CREATE TABLE kafka_binlog ( " +
                "  user_id INT, " +
                "  user_name STRING, " +
                "`proc_time` as PROCTIME()," +
                "  PRIMARY KEY (user_id) NOT ENFORCED" +
                ") WITH ( " +
                "  'connector' = 'upsert-kafka', " +
                "  'topic' = 'test2', " +
                "  'properties.bootstrap.servers' = 'hadoop102:9092', " +
                "  'key.format' = 'json' ," +
                "  'value.format' = 'json' " +
                ")" +
                "");


        // 3. 从SourceTable 查询数据, 并写入到 SinkTable
         tableEnv.executeSql("insert into kafka_binlog select * from table_name");

         tableEnv.executeSql("select * from kafka_binlog").print();

        env.execute();
    }

}
