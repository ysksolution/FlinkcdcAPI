package FlinkCDC_CODE;

import com.alibaba.fastjson.JSONObject;
import com.ververica.cdc.connectors.mysql.source.MySqlSource;
import com.ververica.cdc.connectors.mysql.table.StartupOptions;
import com.ververica.cdc.debezium.JsonDebeziumDeserializationSchema;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.contrib.streaming.state.EmbeddedRocksDBStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.flink.util.Collector;

import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Properties;

public class CameraAlert {

    public static void main(String[] args) throws Exception {

        String topic = "alert";
        //1.创建执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(3);

        //2.Flink-CDC将读取binlog的位置信息以状态的方式保存在CK,如果想要做到断点续传,需要从Checkpoint或者Savepoint启动程序
        //2.1 开启Checkpoint,每隔10分鐘做一次CK
        env.enableCheckpointing(100*6000L, CheckpointingMode.EXACTLY_ONCE);//头和头的之间
        env.getCheckpointConfig().setCheckpointTimeout(10 * 60000L);

//        env.enableCheckpointing(1000L, CheckpointingMode.EXACTLY_ONCE);//头和头的之间
//        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(1000L);//头和尾

        //2.3 设置任务关闭的时候保留最后一次CK数据
        env.getCheckpointConfig().setExternalizedCheckpointCleanup(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);
        env.getCheckpointConfig().setMaxConcurrentCheckpoints(1);
        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(100*6000L);//头和尾


        //2.4 指定从CK自动重启策略
        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(2, 5000));

        //2.5 设置状态后端
//        env.setStateBackend(new HashMapStateBackend());
        //开启增量检查点
        env.setStateBackend(new EmbeddedRocksDBStateBackend(true));
        env.getCheckpointConfig().enableUnalignedCheckpoints(false);

        // 3. 设置 checkpoint 的存储路径
//        env.getCheckpointConfig().setCheckpointStorage("hdfs://hadoop103:8020/ck/" + topic);
        env.getCheckpointConfig().setCheckpointStorage("hdfs://mycluster:8020/ck/" + topic);

        //2.6 设置访问HDFS的用户名
        System.setProperty("HADOOP_USER_NAME", "sarah");


        //4.定义jdbc配置
        //构建mysqlSource
        MySqlSource mysqlCdcSource =
                MySqlSource.<String>builder()
                        .hostname("hadoop102")
                        .port(3306)
                        .username("root")
                        .password("mivbAs7Awc")
                        .databaseList("gmall")
                        .tableList(
                                "gmall.user_info"
                        )
                        .serverTimeZone("Asia/Shanghai")
                        .startupOptions(StartupOptions.latest())
                        .scanNewlyAddedTableEnabled(true)
//                        .deserializer(new CustomerDeserialization())
                        .deserializer(new JsonDebeziumDeserializationSchema()) // converts SourceRecord to JSON String
                        .build();
//

        //使用CDC Source从MySQL读取数据
        DataStreamSource mysqlSource = env.fromSource(mysqlCdcSource, WatermarkStrategy.noWatermarks(), "MysqlSource");



        // 对读取到的数据进行处理，调用sendAlert方法
        mysqlSource.addSink(new SinkFunction<String>() {
            @Override
            public void invoke(String value, Context context) {
                JSONObject obj = JSONObject.parseObject(value);
                sendAlert(obj.getJSONObject("after").toJSONString());
            }
        });
//        mysqlSource.print();

        env.execute();

    }

    public static void sendAlert(String info){
        String urlString = "https://qyapi.weixin.qq.com/cgi-bin/webhook/send?key=813fef";

        String jsonInputString = "{\n" +
                "    \"msgtype\": \"text\",\n" +
                "    \"text\": {\n" +
                "        \"content\": \"" + escapeJson(info) + "\",\n" +
                "        \"mentioned_list\":[\"SARAH.X\",\"@all\"]\n" +
                "    }\n" +
                "}";

//        System.out.println(jsonInputString); // 打印发送的JSON内容，以便调试
        try {
            // 创建URL对象
            URL url = new URL(urlString);
            HttpURLConnection connection = (HttpURLConnection) url.openConnection();

            // 设置请求方法为POST
            connection.setRequestMethod("POST");
            // 设置请求头
            connection.setRequestProperty("Content-Type", "application/json; utf-8");
            connection.setRequestProperty("Accept", "application/json");
            // 允许写入请求体
            connection.setDoOutput(true);

            // 写入请求体
            try (OutputStream os = connection.getOutputStream()) {
                byte[] input = jsonInputString.getBytes(StandardCharsets.UTF_8);
                os.write(input, 0, input.length);
            }
            // 处理响应
            int code = connection.getResponseCode();
//            System.out.println("Response Code: " + code);

        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public static String escapeJson(String str) {
        return str.replace("\"", "\\\"");
    }

}
