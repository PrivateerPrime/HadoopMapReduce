package com.hadoop;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class Etap2a {

    public static class Etap2aMapper1
            extends Mapper<Object, Text, Text, Text> {

        @Override
        protected void map(Object key, Text value,
                           Mapper<Object, Text, Text, Text>.Context context)
                throws IOException, InterruptedException {

            String[] data = value.toString().split("\t");

            context.write(new Text(data[0]), new Text("category: " + data[1]));
        }
    }

    public static class Etap2aMapper2
            extends Mapper<Object, Text, Text, Text> {

        @Override
        protected void map(Object key, Text value,
                           Mapper<Object, Text, Text, Text>.Context context)
                throws IOException, InterruptedException {

            if (value.toString().startsWith("Invoice")) {
                return; //TODO Ugly, better exit strategy
            }

            String[] data = Utils.parseCSVLine(value.toString());

            Text invoice = new Text(data[0]);

            String month = data[1].substring(0, 2);
            String city = data[5];
            String sales = data[22];
            String point = !data[7].isEmpty() ? data[7] : "POINT (null,null)";

            context.write(invoice, new Text("data: " + month + "\t" + city + "\t" + sales + "\t" + point + "\t"));
        }
    }

    public static class Etap2aReducer extends Reducer<Text, Text, Text, Text> {

        @Override
        protected void reduce(Text key, Iterable<Text> values,
                              Reducer<Text, Text, Text, Text>.Context context)
                throws IOException, InterruptedException {

            StringBuilder resultValue = new StringBuilder();

            for (Text value : values) {
                String[] result = value.toString().split(": ");
                if (result[0].equals("category")) {
                    resultValue.append(result[1]).append("\t");
                } else if (result[0].equals("data")) {
                    resultValue.insert(0, result[1]);
                } else {
                    throw new RuntimeException("WTF");
                }

            }

            context.write(key, new Text(resultValue.toString()));
        }
    }


    public static void main(String[] args) {
        try {
            Configuration conf = new Configuration();
            if (args.length != 4) {
                System.err.println("Incorrect number of arguments");
                System.exit(2);
            }
            Job job = new Job(conf, "Etap2a Job");
            job.setJarByClass(Etap2a.class);
            job.setReducerClass(Etap2aReducer.class);
            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(Text.class);

            MultipleInputs.addInputPath(job, new Path(args[1]), TextInputFormat.class, Etap2aMapper1.class);
            MultipleInputs.addInputPath(job, new Path(args[2]), TextInputFormat.class, Etap2aMapper2.class);
            FileOutputFormat.setOutputPath(job, new Path(args[3]));

            System.exit(job.waitForCompletion(true) ? 0 : 1);
        } catch (IOException | InterruptedException | ClassNotFoundException e) {
            e.printStackTrace();
            System.exit(1);
        }
    }
}
