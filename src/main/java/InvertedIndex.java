import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.math.BigDecimal;
import java.net.URI;
import java.util.*;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.partition.HashPartitioner;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.StringUtils;

public class InvertedIndex {
    /**
     * Mapper部分
     **/
    public static class InvertedIndexMapper extends Mapper<Object, Text, Text, IntWritable> {
        private final static IntWritable one = new IntWritable(1);//常量1

        private boolean caseSensitive;
        private Set<String> patternsToSkip = new HashSet<String>();

        private Configuration conf;
        private BufferedReader fis;
        @Override
        public void setup(Context context) throws IOException,
                InterruptedException {
            conf = context.getConfiguration();
            caseSensitive = conf.getBoolean("inverted.case.sensitive", true);
            if (conf.getBoolean("inverted.skip.patterns", false)) {
                URI[] patternsURIs = Job.getInstance(conf).getCacheFiles();
                for (URI patternsURI : patternsURIs) {
                    Path patternsPath = new Path(patternsURI.getPath());
                    String patternsFileName = patternsPath.getName().toString();
                    parseSkipFile(patternsFileName);
                }
            }
        }

        private void parseSkipFile(String fileName) {
            try {
                fis = new BufferedReader(new FileReader(fileName));
                String pattern = null;
                while ((pattern = fis.readLine()) != null) {
                    patternsToSkip.add(pattern);
                }
            } catch (IOException ioe) {
                System.err.println("Caught exception while parsing the cached file '"
                        + StringUtils.stringifyException(ioe));
            }
        }

        public static boolean isNumeric(String str) {
            String bigStr;
            try {
                bigStr = new BigDecimal(str).toString();
            } catch (Exception e) {
                return false;//异常 说明包含非数字。
            }
            return true;
        }
        /**
         * 对输入的Text切分为多个word,每个word作为一个key输出
         * 输入：key:当前行偏移位置, value:当前行内容
         * 输出：key:word#filename, value:1
         */
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            FileSplit fileSplit = (FileSplit) context.getInputSplit();
            String fileName = fileSplit.getPath().getName().toLowerCase();//获取文件名，为简化下一步对文件名处理，转换为小写
            int pos = fileName.indexOf(".");
            if (pos > 0) {
                fileName = fileName.substring(0, pos);//去除文件名后缀
            }
            Text word = new Text();
            String line = (caseSensitive) ?
                    value.toString() : value.toString().toLowerCase();
            for (String pattern : patternsToSkip) {
                line = line.replaceAll(pattern, "");
            }
            StringTokenizer itr = new StringTokenizer(line);
            while (itr.hasMoreTokens()) {
                word.set(itr.nextToken());
                int length = word.toString().length();
                if(length < 3) continue;
                if(isNumeric(word.toString())) continue;

                word.set(word.toString() + "#" + fileName);
                context.write(word, one);//输出 word#filename 1
            }
        }
    }

    /**
     * Combiner部分
     **/
    public static class SumCombiner extends Reducer<Text, IntWritable, Text, IntWritable> {
        private IntWritable result = new IntWritable();

        /**
         * 将Mapper输出的中间结果相同key部分的value累加，减少向Reduce节点传输的数据量
         * 输出：key:word#filename, value:累加和
         */
        public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            int sum = 0;
            for (IntWritable val : values) {
                sum += val.get();
            }
            result.set(sum);
            context.write(key, result);
        }
    }

    /**
     * Partitioner部分
     **/
    public static class NewPartitioner extends HashPartitioner<Text, IntWritable> {
        /**
         * 为了将同一个word的键值对发送到同一个Reduce节点，对key进行临时处理
         * 将原key的(word, filename)临时拆开，使Partitioner只按照word值进行选择Reduce节点
         */
        public int getPartition(Text key, IntWritable value, int numReduceTasks) {
            String term = key.toString().split("#")[0];//获取word#filename中的word
            return super.getPartition(new Text(term), value, numReduceTasks);
        }
    }

    /**
     * Reducer部分
     **/
    public static class InvertedIndexReducer extends Reducer<Text, IntWritable, Text, Text> {
        private String term = new String();//临时存储word#filename中的word
        private String last = " ";//临时存储上一个word
        private Map<Text, IntWritable> countMap = new HashMap<>();//临时存储输出的value部分
        Map<Text, IntWritable> sortedMap = new HashMap<>();
        StringBuilder out = new StringBuilder();

        /**
         * 利用每个Reducer接收到的键值对中，word是排好序的
         * 只需要将相同的word中，将(word,filename)拆分开，将filename与累加和拼到一起，存储到临时Hashmap中，排好序再放在临时StringBuilder中
         * 待出现word不同，则将此word作为key，存储有此word出现的全部filename及其出现次数的StringBuilder作为value输出
         * 输入：key:word#filename, value:[NUM,NUM,...]
         * 输出：key:word, value:filename#NUM,filename#NUM,...
         */
        public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            term = key.toString().split("#")[0];//获取word
            if (!term.equals(last)) {//此次word与上次不一样，则将上次进行处理并输出
                if (!last.equals(" ")) {//避免第一次比较时出错
                    sortedMap = MiscUtils.sortByValues(countMap);
                    for (Text skey : sortedMap.keySet())
                        out.append(skey.toString()+"#"+sortedMap.get(skey).toString()+',');
                    out.setLength(out.length() - 1);
                    context.write(new Text(last), new Text(String.format("%s", out.toString())));//value部分拼接后输出
                    countMap = new HashMap<>();
                    out = new StringBuilder();
                }
                last = term;//更新word，为下一次做准备
            }
            int sum = 0;//累加相同word和filename中出现次数
            for (IntWritable val : values) {
                sum += val.get();
            }
            countMap.put(new Text(key.toString().split("#")[1]), new IntWritable(sum));//将filename#NUM, 临时存储
        }

        /**
         * 上述reduce()只会在遇到新word时，处理并输出前一个word，故对于最后一个word还需要额外的处理
         * 重载cleanup()，处理最后一个word并输出
         */
        public void cleanup(Context context) throws IOException, InterruptedException {
            sortedMap = MiscUtils.sortByValues(countMap);
            for (Text skey : sortedMap.keySet())
                out.append(skey.toString()+"#"+sortedMap.get(skey).toString()+',');
            out.setLength(out.length() - 1);
            context.write(new Text(last), new Text(String.format("%s", out.toString())));
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        GenericOptionsParser optionParser = new GenericOptionsParser(conf, args);
        String[] remainingArgs = optionParser.getRemainingArgs();
        if ((remainingArgs.length != 2) && (remainingArgs.length != 4) && (remainingArgs.length != 6)) {
            System.err.println("Usage: invertedIndex <in> <out> [-skip skipPatternFile] [-skip skipPatternFile]");
            System.exit(2);
        }
        Job job = new Job(conf, "inverted index");
        job.setJarByClass(InvertedIndex.class);
        job.setMapperClass(InvertedIndexMapper.class);
        job.setCombinerClass(SumCombiner.class);
        job.setReducerClass(InvertedIndexReducer.class);
        job.setNumReduceTasks(1);//设定使用Reduce节点个数
        job.setPartitionerClass(NewPartitioner.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        List<String> otherArgs = new ArrayList<String>();
        for (int i=0; i < remainingArgs.length; ++i) {
            if ("-skip".equals(remainingArgs[i])) {
                job.addCacheFile(new Path(remainingArgs[++i]).toUri());
                job.getConfiguration().setBoolean("inverted.skip.patterns", true);
            } else {
                otherArgs.add(remainingArgs[i]);
            }
        }
        FileInputFormat.addInputPath(job, new Path(otherArgs.get(0)));
        FileOutputFormat.setOutputPath(job, new Path(otherArgs.get(1)));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}