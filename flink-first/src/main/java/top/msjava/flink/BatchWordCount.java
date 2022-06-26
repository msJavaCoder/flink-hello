package top.msjava.flink;

import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.AggregateOperator;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.operators.FlatMapOperator;
import org.apache.flink.api.java.operators.UnsortedGrouping;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;

/**
 * @BelongsProject: flink-hello
 * @BelongsPackage: top.msjava.flink
 * @Author: msJava
 * @CreateTime: 2022-06-26  16:35
 * @Description: 批处理-统计文本单词出现次数
 * @Version: 1.0
 */
public class BatchWordCount {

    public static final String FILE_PATH = "input/words.txt";

    public static void main(String[] args) throws Exception {
        // 1. 创建执行环境
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        // 2. 从文件中读取数据：按行读取(存储的元素就是每行文本)
        DataSource<String> source = env.readTextFile(FILE_PATH);
        // 3. 转换数据格式
        FlatMapOperator<String, Tuple2<String, Long>> wordAndOne = source
                .flatMap((String line, Collector<Tuple2<String, Long>> out) -> {
                    String[] words = line.split(" ");
                    for (String word : words) {
                        out.collect(Tuple2.of(word, 1L));
                    }
                    // 当Lambda表达式使用Java泛型的时候，由于泛型擦除的存在而需要显示的声明类型信息。
                }).returns(Types.TUPLE(Types.STRING, Types.LONG));
        // 4. 按照word进行分组
        UnsortedGrouping<Tuple2<String, Long>> wordAndOneUG = wordAndOne.groupBy(0);
        // 5. 分组内聚合统计
        AggregateOperator<Tuple2<String, Long>> sum = wordAndOneUG.sum(1);
        // 6. 打印结果
        sum.print();
    }
}
