package FlinkCDC_CODE;

import com.ververica.cdc.connectors.mysql.source.MySqlSource;
import com.ververica.cdc.connectors.mysql.table.StartupOptions;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.contrib.streaming.state.EmbeddedRocksDBStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.flink.util.Collector;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Properties;

public class FlinkCDC {

    public static void main(String[] args) throws Exception {

        String topic = "db_core";

//        String brokers = "hadoop105:9092,hadoop106:9092,hadoop104:9092";
        String brokers = "hadoop102:9092,hadoop103:9092,hadoop104:9092";

        //1.创建执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(3);

        //2.Flink-CDC将读取binlog的位置信息以状态的方式保存在CK,如果想要做到断点续传,需要从Checkpoint或者Savepoint启动程序
        //2.1 开启Checkpoint,每隔10分鐘做一次CK
//        env.enableCheckpointing(100*6000L, CheckpointingMode.EXACTLY_ONCE);//头和头的之间
//        env.getCheckpointConfig().setCheckpointTimeout(10 * 60000L);

        env.enableCheckpointing(1000L, CheckpointingMode.EXACTLY_ONCE);//头和头的之间
        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(1000L);//头和尾

        //2.3 设置任务关闭的时候保留最后一次CK数据
        env.getCheckpointConfig().setExternalizedCheckpointCleanup(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);
        env.getCheckpointConfig().setMaxConcurrentCheckpoints(1);
//        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(100*6000L);//头和尾


        //2.4 指定从CK自动重启策略
        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(2, 5000));

        //2.5 设置状态后端
//        env.setStateBackend(new HashMapStateBackend());
        //开启增量检查点
        env.setStateBackend(new EmbeddedRocksDBStateBackend(true));
        env.getCheckpointConfig().enableUnalignedCheckpoints(false);

        // 3. 设置 checkpoint 的存储路径
        env.getCheckpointConfig().setCheckpointStorage("hdfs://hadoop104:8020/ck/" + topic);
//        env.getCheckpointConfig().setCheckpointStorage("hdfs://mycluster:8020/ck/" + topic);

        //2.6 设置访问HDFS的用户名
        System.setProperty("HADOOP_USER_NAME", "sarah");

        //3.创建Flink-MySQL-CDC的Source
        //自定义时间转换配置
        Properties properties = new Properties();
        properties.setProperty("converters", "dateConverters");
        properties.setProperty("dateConverters.type", "flink.MySqlDateTimeConverter");


        //4.定义jdbc配置

//        MySqlSource mysqlCdcSource = MySqlSource.<String>builder()
//                .hostname("47.52.185.61")
//                .port(3306)
//                .username("dw_readonly")
//                .password("jjhpM#b#Z0")
//                .serverTimeZone("America/Los_Angeles")
//                .databaseList("db_sjfood")
//                .tableList("db_sjfood.tb_010_sjfood_zsd01")
//                .debeziumProperties(properties)
//                .scanNewlyAddedTableEnabled(true)
//                .deserializer(new CustomerDeserialization()) // converts SourceRecord to JSON String
//                .build();
//

        //构建mysqlSource
        MySqlSource mysqlCdcSource =
                MySqlSource.<String>builder()
                        .hostname("hadoop102")
                        .port(3306)
                        .username("root")
                        .password("xxx")
                .databaseList("easypm")
                .tableList(
                        "easypm.ads_group_result_stat",
                        "easypm.ads_confirmed_requirement",
                        "easypm.ads_resouces",
                        "easypm.ads_requirement_to_be_confirmed",
                        "easypm.ads_hdb_quota"
                )
// 可选配置项,如果不指定该参数,则会读取上一个配置下的所有表的数据，注意：指定的时候需要使用"db.table"的方式
//                        .databaseList("student")
//                        .tableList("student.order_name","student.table_name","student.table_name2")
//                        .tableList("student.order_name","student.table_name")
                        .serverTimeZone("Asia/Shanghai")
                        .debeziumProperties(properties)
                        .startupOptions(StartupOptions.initial())

                        .scanNewlyAddedTableEnabled(true)
                        .deserializer(new CustomerDeserialization())
//                        .deserializer(new JsonDebeziumDeserializationSchema()) // converts SourceRecord to JSON String
                        .build();
//

        //使用CDC Source从MySQL读取数据
        env.fromSource(mysqlCdcSource, WatermarkStrategy.noWatermarks(), "MysqlSource").print();


//        //打印数据并将数据写入 Kafka
//        mysqlSourceDS.addSink(getKafkaProducer(brokers, topic)).name(topic).uid(topic + "uid3");
//
//        mysqlSourceDS
//                .flatMap(new FlatMapFunction<String, Tuple2<String, Integer>>() {
//                    @Override
//                    public void flatMap(String value, Collector<Tuple2<String, Integer>> out) throws Exception {
//                        String[] data = value.split("\"table\"");
//                        data[1] = "table" + data[1];
//                        String[] data2 = data[1].split(",");
//                        out.collect(Tuple2.of(data2[0], 1));
//                    }
//                })
//                .keyBy(value -> value.f0) // 使用 f0 作为 key
////                .window(SlidingProcessingTimeWindows.of(Time.minutes(10), Time.minutes(10))) // 5分钟的窗口，每5秒滑动一次
//                .window(SlidingProcessingTimeWindows.of(Time.milliseconds(10), Time.milliseconds(10))) // 5分钟的窗口，每5秒滑动一次
//                .apply(new CustomWindowFunction())
//                .print();

        env.execute("FlinkCDC");

    }
        private static class CustomWindowFunction implements WindowFunction<Tuple2<String, Integer>, Tuple3<String, String, Integer>, String, TimeWindow> {
            @Override
            public void apply(String key, TimeWindow window, Iterable<Tuple2<String, Integer>> input, Collector<Tuple3<String, String, Integer>> out) throws Exception {
                String star = "[".concat(new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(new Date(window.getStart()))).concat("-").concat(new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(new Date(window.getEnd()))).concat("]");
                int count = 0;
                for (Tuple2<String, Integer> element : input) {
                    count += element.f1;
                }
                out.collect(Tuple3.of(star, key, count));
            }
        }

    //kafka 生产者
    public static FlinkKafkaProducer<String> getKafkaProducer(String brokers,String topic) {
        Properties props = new Properties();
        props.setProperty("bootstrap.servers",brokers);
        props.put("buffer.memory", 53554432);
        props.put("batch.size", 0);
        props.put("linger.ms", 10);
        props.put("max.request.size", 10485760);
        props.put("acks", "1");
        props.put("retries", 10);
        props.put("retry.backoff.ms", 500);

        FlinkKafkaProducer<String> producer = new FlinkKafkaProducer(topic,new SimpleStringSchema(), props);
        return producer;
    }

}

