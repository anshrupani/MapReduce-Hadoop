package solutions.assignment2;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import examples.MapRedFileUtils;
import examples.wordcount.MapRedWordFrequencyCount.MapRecords;
import examples.wordcount.MapRedWordFrequencyCount.ReduceRecords;

import java.text.SimpleDateFormat;
import java.util.Date;
public class MapRedSolution2
{
	public static String out;
	
    /* your code goes in here*/
    public static String changeTime(String time) throws Exception {
        try {       
            SimpleDateFormat format24 = new SimpleDateFormat("HH:mm");
            SimpleDateFormat format12 = new SimpleDateFormat("ha");
            Date format24dt = format24.parse(time);
            out = format12.format(format24dt);
        } catch (Exception e) {
            e.printStackTrace();
        }
         return out.toLowerCase();
    }
    
    
    public static class MapRecords extends Mapper<LongWritable, Text, Text, IntWritable>
    {
        private final static IntWritable one = new IntWritable(1);
        private Text word = new Text();
        
        @Override
        protected void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException
        {    
			try {
            String data = value.toString();
            String[] taxiSplitData = data.split(",");
            String timeData = taxiSplitData[1];
			String timeOnlyPickupHour  = timeData.substring(11,13);
//			System.out.println(timeOnlyPickupHour);
//          Reconfirm if the data in the substring is an Integer-->
			if(timeOnlyPickupHour.matches("\\d+")) {

				word.set(changeTime(timeOnlyPickupHour+":00"));
				context.write(word, one);
			}
			} catch (Exception e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
        }
    }

    public static class ReduceRecords extends Reducer<Text, IntWritable, Text, IntWritable>
    {
        private IntWritable result = new IntWritable();

        @Override
        protected void reduce(Text key, Iterable<IntWritable> values,
            Context context) throws IOException, InterruptedException
        {
            int sum = 0;
        
            for (IntWritable val : values)
            sum += val.get();
            
            result.set(sum);
            context.write(key, result);
        }
    }
	
    public static void main(String[] args) throws Exception
    {
        Configuration conf = new Configuration();

        String[] otherArgs =
            new GenericOptionsParser(conf, args).getRemainingArgs();
        
        if (otherArgs.length != 2)
        {
            System.err.println("Usage: MapRedSolution2 <in> <out>");
            System.exit(2);
        }
        
        Job job = Job.getInstance(conf, "MapRed Solution #2");
        
        /* your code goes in here*/
        job.setMapperClass(MapRecords.class);
        job.setCombinerClass(ReduceRecords.class);
        job.setReducerClass(ReduceRecords.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        
        FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
        FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
        
        MapRedFileUtils.deleteDir(otherArgs[1]);
        int exitCode = job.waitForCompletion(true) ? 0 : 1; 

        FileInputStream fileInputStream = new FileInputStream(new File(otherArgs[1]+"/part-r-00000"));
        String md5 = org.apache.commons.codec.digest.DigestUtils.md5Hex(fileInputStream);
        fileInputStream.close();
        
        String[] validMd5Sums = {"03357cb042c12da46dd5f0217509adc8", "ad6697014eba5670f6fc79fbac73cf83", "07f6514a2f48cff8e12fdbc533bc0fe5", 
            "e3c247d186e3f7d7ba5bab626a8474d7", "fce860313d4924130b626806fa9a3826", "cc56d08d719a1401ad2731898c6b82dd", 
            "6cd1ad65c5fd8e54ed83ea59320731e9", "59737bd718c9f38be5354304f5a36466", "7d35ce45afd621e46840627a79f87dac"};
        
        for (String validMd5 : validMd5Sums) 
        {
            if (validMd5.contentEquals(md5))
            {
                System.out.println("The result looks good :-)");
                System.exit(exitCode);
            }
        }
        System.out.println("The result does not look like what we expected :-(");
        System.exit(exitCode);
    }
}
