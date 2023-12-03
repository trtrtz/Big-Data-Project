import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
public class UniqueRecs {

        public class UniqueRecsMapper
            extends Mapper<Object, Text, Text, NullWritable>{

        public void map(Object key, Text value, Context context
        ) throws IOException, InterruptedException {
            String line = value.toString();
            String [] arr = line.split(",(?=([^\"]*\"[^\"]*\")*[^\"]*$)");
            String col = "Title: "+ arr[0] + " Year " + arr[1] + " Bechdel Score " + arr[2] + " Dubious Score "+ arr[3];
            Text columns = new Text(col);
            context.write(columns,NullWritable.get());

            
        }
        }

        public class UniqueRecsReducer
            extends Reducer<Text,NullWritable,Text,NullWritable> {


        public void reduce(Text key, Iterable<NullWritable> values,
                           Context context
        ) throws IOException, InterruptedException {
             context.write(key, NullWritable.get());
        }
        }

        public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "unique recs");
        job.setJarByClass(UniqueRecs.class);
        job.setMapperClass(UniqueRecsMapper.class);
        job.setCombinerClass(UniqueRecsReducer.class);
        job.setReducerClass(UniqueRecsReducer.class);
        job.setNumReduceTasks(1); 
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(NullWritable.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
