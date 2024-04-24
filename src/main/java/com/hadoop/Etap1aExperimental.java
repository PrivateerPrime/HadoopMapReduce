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
import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map.Entry;
import java.util.PriorityQueue;
import java.util.StringTokenizer;

public class Etap1aExperimental {

    public static class AlcoholCategoriesMapper
            extends Mapper<Object, Text, Text, IntWritable> {

        @Override
        protected void map(Object key, Text value,
                           Mapper<Object, Text, Text, IntWritable>.Context context)
                throws IOException, InterruptedException {

            if (value.toString().startsWith("Invoice")) {
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

        private PriorityQueue<Entry<String, Integer>> queue;

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            queue = new PriorityQueue<>(20, Entry.comparingByValue());
        }

        @Override
        public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            int sum = 0;
            for (IntWritable val : values) {
                sum += val.get();
            }

            queue.offer(new AbstractMap.SimpleImmutableEntry<>(key.toString(), sum));
            if (queue.size() > 20) {
                queue.poll(); // removes the element with the smallest count
            }
        }

        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            List<Entry<String, Integer>> topCategories = new ArrayList<>();
            while (!queue.isEmpty()) {
                topCategories.add(queue.poll());
            }
            Collections.reverse(topCategories); // This is to have the output from highest to lowest

            for (Entry<String, Integer> entry : topCategories) {
                context.write(new Text(entry.getKey()), new IntWritable(entry.getValue()));
            }
        }
    }

    public static void main(String[] args) {
        try {
            Configuration conf = new Configuration();
            if (args.length != 3) {
                System.err.println("Incorrect number of arguments");
                System.exit(2);
            }
            Job job = getJob(conf);
            FileInputFormat.addInputPath(job, new Path(args[1]));
            FileOutputFormat.setOutputPath(job, new Path(args[2]));


            System.exit(job.waitForCompletion(true) ? 0 : 1);
        } catch (IOException | InterruptedException | ClassNotFoundException e) {
            e.printStackTrace();
            System.exit(1);
        }
    }

    private static Job getJob(Configuration conf) throws IOException {
        Job job = new Job(conf, "Etap1aExperimental Job");
        job.setJarByClass(Etap1aExperimental.class);
        job.setMapperClass(AlcoholCategoriesMapper.class);
        job.setCombinerClass(AlcoholCategoriesReducer.class);
        job.setReducerClass(AlcoholCategoriesReducer.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        return job;
    }
}



