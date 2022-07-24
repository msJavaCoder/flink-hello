package top.msjava.flink;

import com.ververica.cdc.connectors.mysql.source.MySqlSource;
import com.ververica.cdc.connectors.mysql.table.StartupOptions;
import com.ververica.cdc.debezium.JsonDebeziumDeserializationSchema;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @BelongsProject: flink-hello
 * @BelongsPackage: top.msjava.flink.job
 * @Author: msJava
 * @CreateTime: 2022-07-02  14:31
 * @Description: TODO
 * @Version: 1.0
 */
public class MySqlJob {

    public static final String JOB_NAME = "MYSQL-CDC-JOB";
    public static final String HOST_NAME = "127.0.0.1";
    public static final String USER_NAME = "root";
    public static final String PASSWD = "root";
    public static final String DATABASE = "cdc";
    public static final String TABLE = "t_user";
    public static final Integer PORT = 3307;

    public static final StartupOptions startupOption = StartupOptions.initial();

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        MySqlSource<String> sourceFunction = MySqlSource.<String>builder()
                .hostname(HOST_NAME)
                .port(PORT)
                .username(USER_NAME)
                .password(PASSWD)
                .databaseList(DATABASE)
                .tableList(TABLE)
                .startupOptions(startupOption)
                .deserializer(new JsonDebeziumDeserializationSchema())
                .build();

        DataStreamSource<String> source = env.fromSource(sourceFunction, WatermarkStrategy.noWatermarks(), "mysql-cdc");

        source.print();

        env.execute(JOB_NAME);
    }
}
