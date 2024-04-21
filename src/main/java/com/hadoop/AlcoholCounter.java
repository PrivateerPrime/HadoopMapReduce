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
import org.apache.log4j.Logger;


import java.io.IOException;

public class AlcoholCounter {


    private static final Logger log = Logger.getLogger(AlcoholCounter.class);

    public static class AlcoholCategoriesMapper
            extends Mapper<Object, Text, Text, IntWritable> {

//        private final Text invoice = new Text();
        private final Text alcoholCategory = new Text();

        @Override
        protected void map(Object key, Text value,
                           Mapper<Object, Text, Text, IntWritable>.Context context)
                throws IOException, InterruptedException {

            String[] data = value.toString().split(",");
            for (String dataTemp : data) {
                log.info(dataTemp);
            }

            if (data[0].equals("Invoice/Item Number")) {
                return; //TODO Ugly, better exit strategy
            }

//            invoice.set(data[0]);
            String category = data[11];
            if (category.contains("VODKA")) {
                alcoholCategory.set("VODKA");
            } else if (category.contains("RUM")) {
                alcoholCategory.set("RUM");
            } else if (category.contains("WHISKIES") || category.contains("WHISKEY")) {
                alcoholCategory.set("WHISKEY");
            } else if (category.contains("LIQUEURS") || category.contains("LIQUEUR")) {
                alcoholCategory.set("LIQUEURS");
            } else if (category.contains("TEQUILA")) {
                alcoholCategory.set("TEQUILA");
            } else if (category.contains("COCKTAILS")) {
                alcoholCategory.set("COCKTAILS");
            } else if (category.contains("SCHNAPPS")) {
                alcoholCategory.set("SCHNAPPS");
            } else {
                alcoholCategory.set("MISC");
            }
            context.write(alcoholCategory, new IntWritable(1));
        }
    }

    public static class AlcoholCategoriesReducer extends Reducer<Text, IntWritable, Text, IntWritable> {

        @Override
        protected void reduce(Text alcoholCategory, Iterable<IntWritable> invoices,
                              Reducer<Text, IntWritable, Text, IntWritable>.Context context)
                throws IOException, InterruptedException {
            int result = 0;
            for (IntWritable invoice : invoices) {
                result += invoice.get();
            }

            log.info(alcoholCategory + ": " + result);

            context.write(alcoholCategory, new IntWritable(result));
        }
    }

    public static void main(String[] args) {
        try {
            Configuration conf = new Configuration();
            if (args.length != 3) {
                System.err.println("Incorrect number of arguments");
                System.exit(2);
            }
            Job job = new Job(conf, "AlcoholCategoriesCounter");
            job.setJarByClass(AlcoholCounter.class);
            job.setMapperClass(AlcoholCategoriesMapper.class);
            job.setCombinerClass(AlcoholCategoriesReducer.class);
            job.setReducerClass(AlcoholCategoriesReducer.class);
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
