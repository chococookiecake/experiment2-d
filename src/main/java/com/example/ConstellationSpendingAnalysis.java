import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.LineRecordReader;
import java.text.DecimalFormat;
import java.io.DataInput;
import java.io.DataOutput;
import org.apache.hadoop.io.NullWritable;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Set;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

public class ConstellationSpendingAnalysis {

    public static class SkipFirstLineInputFormat extends TextInputFormat {

        @Override
        public RecordReader<LongWritable, Text> createRecordReader(InputSplit split, TaskAttemptContext context) {
            return new SkipFirstLineRecordReader();  

        }
        public static class SkipFirstLineRecordReader extends LineRecordReader {
            private boolean firstLineSkipped = false;

            @Override
            public boolean nextKeyValue() throws IOException {
                // 跳过第一行
                if (!firstLineSkipped) {
                    firstLineSkipped = true;
                    // 调用一次 nextKeyValue 跳过第一行
                    super.nextKeyValue();  
                    return super.nextKeyValue();  // 返回第二行及以后的内容
                } else {
                    return super.nextKeyValue();  // 正常处理
                }
            }
        }
    }

    public static class UserConstellationMapper extends Mapper<Object, Text, Text, Text> {
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String[] fields = value.toString().split(",");
            if (fields.length >= 4) {
                String userId = fields[0];
                String constellation = fields[3];
                context.write(new Text(userId), new Text("constellation:" + constellation));
            }
        }
    }

    public static class UserSpendingMapper extends Mapper<Object, Text, Text, Text> {
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String[] fields = value.toString().split(",");
            if (fields.length >= 10) {
                String userId = fields[0];
                String date = fields[1];
                String amount = fields[9];
                String month = date.substring(0, 6); // YYYY-MM

                context.write(new Text(userId), new Text("spending:" + month + "," + amount));
            }
        }
    }

    public static class CompositeKey {
        private final String first;
        private final String second;

        public CompositeKey(String first, String second) {
            this.first = first;
            this.second = second;
        }

        public String getfirst() {
            return first;
        }

        public String getsecond() {
            return second;
        }

        @Override
        public boolean equals(Object obj) {
            if (this == obj) return true;
            if (!(obj instanceof CompositeKey)) return false;
            CompositeKey other = (CompositeKey) obj;
            return first.equals(other.first) && second.equals(other.second);
        }

        @Override
        public int hashCode() {
            return Objects.hash(first, second);
        }
    }


    public static class SpendingReducer extends Reducer<Text, Text, NullWritable, Text> {
        private Map< String , String > userConstellationMap = new HashMap<>(); //userid 星座
        private Map< CompositeKey, Double > userMonthlyConsumption = new HashMap<>(); //(userid 月份) 消费额
        private Map< CompositeKey, Double > constellationMonthlyConsumption = new HashMap<>(); //(星座 月份) 累计消费额
        private Map<String, Integer> constellationCount = new HashMap<>(); //星座 人数

        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

            String constellation = "";
            Set<String> uniqmonths = new HashSet<>();

            for (Text val : values) {
                String value = val.toString();
                if (value.startsWith("constellation:")) {
                    constellation= value.substring(14);
                    userConstellationMap.put(key.toString(), constellation);


                } else if (value.startsWith("spending:")) {
                    String[] spendingData = value.substring(9).split(",");
                    if (spendingData.length == 2) {
                        String currentmonth= spendingData[0];
                        uniqmonths.add(currentmonth);
                        double amount = spendingData[1].isEmpty() ? 0 : Double.parseDouble(spendingData[1]);
                        userMonthlyConsumption.putIfAbsent(new CompositeKey(key.toString(), currentmonth), 0.0);
                        userMonthlyConsumption.put(new CompositeKey(key.toString(), currentmonth), 
                            userMonthlyConsumption.get(new CompositeKey(key.toString(), currentmonth) ) + amount); // 一个人一个月的消费额的累加
                    }
                }
            }

            //属于 该星座 的 人数 的累加
            if(!constellation.isEmpty()){
                constellationCount.putIfAbsent(constellation, 0);
                constellationCount.put(constellation, constellationCount.get(constellation) + 1);

                //(星座 月份) 下每个人的 月消费额 的累加
                for (String month : uniqmonths){
                    constellationMonthlyConsumption.putIfAbsent(new CompositeKey(userConstellationMap.get(key.toString()), month),0.0);
                    constellationMonthlyConsumption.put(new CompositeKey(userConstellationMap.get(key.toString()), month),
                        constellationMonthlyConsumption.get(new CompositeKey(userConstellationMap.get(key.toString()), month)) + userMonthlyConsumption.get(new CompositeKey(key.toString(),month)));
                }
            }//只有在有星座数据时才统计
            
            uniqmonths.clear();
        }

        @Override    
        protected void cleanup(Context context) throws IOException, InterruptedException {

            for (CompositeKey ckey : constellationMonthlyConsumption.keySet()) {
                // 遍历(星座 月份)
        
                int count = constellationCount.get(ckey.getfirst()); // 星座人数
                
                double average = (count > 0) ? (constellationMonthlyConsumption.get(ckey) / count) : 0.0; //(星座 月份)的人均消费额

                // 输出格式为 month, constellation, consumption
                context.write(NullWritable.get(), new Text(ckey.getsecond() + ',' + ckey.getfirst() + "," + String.valueOf(average)));
            }
        }
    }
    

   
    public static class SortSpendingMapper extends Mapper<LongWritable, Text, MonthAveragePair, Text> {
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] parts = value.toString().split(",");
            String month = parts[0]; // 获取月份
            String constellation = parts[1]; // 星座
            double average = Double.parseDouble(parts[2]); // 消费平均值
            
            MonthAveragePair monthAveragePair = new MonthAveragePair();
            monthAveragePair.set(Integer.parseInt(month), average); // 将月份转换为整数
            
            context.write(monthAveragePair, new Text(constellation));
        }
    }

    public static class SortSpendingReducer extends Reducer<MonthAveragePair, Text, Text, Text> {
    public void reduce(MonthAveragePair key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        String month = String.valueOf(key.getMonth()); // 月份
        String averageconsumption = String.valueOf(key.getAverage()); // 消费平均值

        for (Text val : values) {
            context.write(new Text(String.valueOf(month) + ":"), new Text(val + "\t" + averageconsumption));
        }
        // 输出格式为 YYYYMM:    constellation   averageconsumption
    }
    }

    public static class MonthAveragePair implements WritableComparable<MonthAveragePair> {
        private int month;      // 存储月份
        private double average;  // 存储消费平均值

        public void set(int month, double average) {
            this.month = month;
            this.average = average;
        }

        public int getMonth() {
            return month;
        }

        public double getAverage() {
            return average;
        }

        @Override
        public void readFields(DataInput in) throws IOException {
            month = in.readInt();
            average = in.readDouble();
        }

        @Override
        public void write(DataOutput out) throws IOException {
            out.writeInt(month);
            out.writeDouble(average);
        }

        @Override
        public int hashCode() {
            return month * 31 + Double.hashCode(average);
        }

        @Override
        public boolean equals(Object right) {
            if (right instanceof MonthAveragePair) {
                MonthAveragePair r = (MonthAveragePair) right;
                return r.month == month && Double.compare(r.average, average) == 0;
            }
            return false;
        }

        @Override
        public int compareTo(MonthAveragePair o) {
            if (month != o.month) {
                return Integer.compare(month, o.month); // 月份升序排列
            }
            return Double.compare(o.average, average); // 消费平均值降序排列
        }
    }

    public static class MonthPartitioner extends Partitioner<MonthAveragePair, Text> {
    @Override
    public int getPartition(MonthAveragePair key, Text value, int numPartitions) {
 
        int month = key.getMonth(); 
        return month % numPartitions; // 按月份进行分区
    }
    }

    public static void main(String[] args) throws Exception {
    Configuration conf1 = new Configuration();

    Job job1 = Job.getInstance(conf1, "Constellation Spending Analysis");
    job1.setJarByClass(ConstellationSpendingAnalysis.class);
    job1.setReducerClass(SpendingReducer.class);
    job1.setOutputKeyClass(Text.class);
    job1.setOutputValueClass(Text.class);
    job1.setInputFormatClass(SkipFirstLineInputFormat.class);       // 获取月份部分
    MultipleInputs.addInputPath(job1, new Path(args[0]), SkipFirstLineInputFormat.class, UserConstellationMapper.class);
    MultipleInputs.addInputPath(job1, new Path(args[1]), SkipFirstLineInputFormat.class, UserSpendingMapper.class);
    TextOutputFormat.setOutputPath(job1, new Path(args[2] + "/tmp"));


    job1.waitForCompletion(true);

    Configuration conf2 = new Configuration();
    Job job2 = Job.getInstance(conf2, "Sort Constellation Spending");
    job2.setJarByClass(ConstellationSpendingAnalysis.class);
    job2.setMapperClass(SortSpendingMapper.class);
    job2.setReducerClass(SortSpendingReducer.class);
    job2.setMapOutputKeyClass(MonthAveragePair.class);
    job2.setOutputKeyClass(Text.class);
    job2.setOutputValueClass(Text.class);


    TextInputFormat.addInputPath(job2, new Path(args[2] + "/tmp"));
    TextOutputFormat.setOutputPath(job2, new Path(args[2] + "/final"));


    job2.setPartitionerClass(MonthPartitioner.class);
    
    System.exit(job2.waitForCompletion(true) ? 0 : 1);
}
}
