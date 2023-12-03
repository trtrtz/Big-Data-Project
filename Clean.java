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

public class Clean {
        public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "clean");
        job.setJarByClass(Clean.class);
        job.setMapperClass(CleanMapper.class);
        job.setCombinerClass(CleanReducer.class);
        job.setReducerClass(CleanReducer.class);
        job.setNumReduceTasks(1); 
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(NullWritable.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}

public class CleanMapper
            extends Mapper<Object, Text, Text, NullWritable>{


        public void map(Object key, Text value, Context context
        ) throws IOException, InterruptedException {
            String line = value.toString();
            String [] arr = line.split(",(?=([^\"]*\"[^\"]*\")*[^\"]*$)");
            //keep title, rating, dubious quality, imdbid and date submitted. i.e. drop id, submitterid, and visible
            if (arr.length == 9) {
                if (arr[1].matches("\\d+") && arr[2].matches("[0-3]") && 
                    arr[3].matches("^(1\\.0|0\\.0)$") && arr[4].matches("\\d+") && arr[7].length() > 4) {
                    String [] newcol = new String[] {arr[0],arr[1],arr[2],arr[3],arr[4],arr[7].substring(0,4)};
                    String row = String.join(",", newcol);
                    Text nrow = new Text(row);
                    context.write(nrow,NullWritable.get());
                }
            } 
        }
}
