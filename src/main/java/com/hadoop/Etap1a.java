package com.hadoop;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.util.StringTokenizer;

public class Etap1a {

    public static class AlcoholCategoriesMapper
            extends Mapper<Object, Text, Text, IntWritable> {

        @Override
        protected void map(Object key, Text value,
                           Mapper<Object, Text, Text, IntWritable>.Context context)
                throws IOException, InterruptedException {

            if (value.toString().startsWith("Inv")) {
                return; //TODO Ugly, better exit strategy
            }

            String[] data = Utils.parseCSVLine(value.toString());

            StringTokenizer itr = new StringTokenizer(data[11]);
            while (itr.hasMoreTokens()) {
                Text word = new Text(itr.nextToken());
                context.write(word, new IntWritable(1));
            }
        }
    }

    public static class AlcoholCategoriesReducer extends Reducer<Text, IntWritable, Text, IntWritable> {

        @Override
        protected void reduce(Text word, Iterable<IntWritable> values,
                              Reducer<Text, IntWritable, Text, IntWritable>.Context context)
                throws IOException, InterruptedException {
            int result = 0;
            for (IntWritable invoice : values) {
                result += invoice.get();
            }

            context.write(word, new IntWritable(result));
        }
    }

    public static void main(String[] args) {
        try {
            Configuration conf = new Configuration();
            if (args.length != 3) {
                System.err.println("Incorrect number of arguments");
                System.exit(2);
            }
            Job job = new Job(conf, "Etap1a Job");
            job.setJarByClass(Etap1a.class);
            job.setMapperClass(Etap1a.AlcoholCategoriesMapper.class);
            job.setCombinerClass(Etap1a.AlcoholCategoriesReducer.class);
            job.setReducerClass(Etap1a.AlcoholCategoriesReducer.class);
            job.setMapOutputKeyClass(Text.class);
            job.setMapOutputValueClass(IntWritable.class);
            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(IntWritable.class);
            FileInputFormat.addInputPath(job, new Path(args[1]));
            FileOutputFormat.setOutputPath(job, new Path(args[2]));


            System.exit(job.waitForCompletion(true) ? 0 : 1);
        } catch (IOException | InterruptedException | ClassNotFoundException e) {
            e.printStackTrace();
            System.exit(1);
        }
    }
}



