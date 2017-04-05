import java.io.IOException;
import java.util.StringTokenizer;
import java.util.ArrayList;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class equijoin {

    public static class TokenizerMapper extends Mapper<Object, Text, Text, Text> {

        private final static IntWritable one = new IntWritable(1);
        private Text word = new Text();

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {

            String ip = value.toString();
            String[] ls = ip.split(",");


            word.set(ls[1]);

            context.write(word, value);

        }
    }

    public static class IntSumReducer extends Reducer<Text, Text, Text, Text> {
        private Text result = new Text();

        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

            ArrayList<String> opr = new ArrayList<String>();
            ArrayList<String> ops = new ArrayList<String>();
            String t1="",t2="";

            for (Text val : values) {


                String[] s = val.toString().split(",");

                if(t1.equals(""))
                {
                    t1=s[0];
                }



                if (s[0].equals(t1)) {

                    ops.add(val.toString());
                } else {
                    opr.add(val.toString());
                }

            }




            int index = 0;
            int i = 0;
            String res = "";





    while (opr.size() > index) {
        i = 0;
        while (ops.size() > i) {
            res = "";
            res = opr.get(index) + "," + ops.get(i);
            ++i;
            result.set(res.toString());
            context.write(null, new Text(res));
        }
        ++index;
    }


        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "equijoin");
        job.setJarByClass(equijoin.class);
        job.setMapperClass(TokenizerMapper.class);
        job.setReducerClass(IntSumReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
