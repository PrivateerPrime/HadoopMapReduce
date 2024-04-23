package com.hadoop;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.thirdparty.com.google.common.collect.ComparisonChain;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

public class Etap2b {

    public static class CompositeKey implements WritableComparable<CompositeKey> {

        private Text month;
        private Text city;

        public CompositeKey() {
            this.month = new Text();
            this.city = new Text();
        }

        public CompositeKey(Text month, Text city) {
            this.month = month;
            this.city = city;
        }

        @Override
        public int compareTo(CompositeKey o) {
            return ComparisonChain.start()
                    .compare(this.month, o.month)
                    .compare(this.city, o.city).result();
        }

        @Override
        public void write(DataOutput dataOutput) throws IOException {
            this.month.write(dataOutput);
            this.city.write(dataOutput);
        }

        @Override
        public void readFields(DataInput dataInput) throws IOException {
            this.month.readFields(dataInput);
            this.city.readFields(dataInput);
        }

        @Override
        public String toString() {
            return month + "\t" + city;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            CompositeKey that = (CompositeKey) o;
            return Objects.equals(month, that.month) && Objects.equals(city, that.city);
        }

        @Override
        public int hashCode() {
            return Objects.hash(month, city);
        }
    }

    public static class Etap2bMapper extends Mapper<Object, Text, CompositeKey, Text> {

        @Override
        protected void map(Object key, Text value,
                           Mapper<Object, Text, CompositeKey, Text>.Context context)
                throws IOException, InterruptedException {

            String[] data = value.toString().split("\t");

            CompositeKey compositeKey = new CompositeKey(new Text(data[1]), new Text(data[2]));
            context.write(compositeKey, new Text(data[3] + "\t" + data[4] + "\t" + data[5]));
        }
    }

    public static class Etap2aReducer extends Reducer<CompositeKey, Text, CompositeKey, Text> {

        @Override
        protected void reduce(CompositeKey key, Iterable<Text> values,
                              Reducer<CompositeKey, Text, CompositeKey, Text>.Context context)
                throws IOException, InterruptedException {

            Double longitude = null;
            Double latitude = null;

            double totalSales = 0.0;

            HashMap<String, Integer> categoryMap = new HashMap<>();

            for (Text value : values) {
                String[] data = value.toString().split("\t");
                totalSales += Double.parseDouble(data[0]);
                String point = data[1];
                if (longitude == null && latitude == null && !point.equals("POINT (null,null)")) {
                    Double[] location = Utils.extractLocation(point);
                    longitude = location[0];
                    latitude = location[1];
                }
                String category = data[2];
                categoryMap.put(category, categoryMap.getOrDefault(category, 0) + 1);
            }

            String mostPopularCategory = categoryMap.entrySet().stream().max(Map.Entry.comparingByValue()).orElseThrow(RuntimeException::new).getKey();

            context.write(key, new Text(longitude + "\t" + latitude + "\t" + mostPopularCategory + "\t" + totalSales));
        }
    }

    public static void main(String[] args) {
        try {
            Configuration conf = new Configuration();
            if (args.length != 3) {
                System.err.println("Incorrect number of arguments");
                System.exit(2);
            }
            Job job = new Job(conf, "Etap2b Job");
            job.setJarByClass(Etap2b.class);
            job.setMapperClass(Etap2b.Etap2bMapper.class);
            job.setReducerClass(Etap2b.Etap2aReducer.class);
            job.setMapOutputKeyClass(CompositeKey.class);
            job.setMapOutputValueClass(Text.class);
            job.setOutputKeyClass(CompositeKey.class);
            job.setOutputValueClass(Text.class);
            FileInputFormat.addInputPath(job, new Path(args[1]));
            FileOutputFormat.setOutputPath(job, new Path(args[2]));


            System.exit(job.waitForCompletion(true) ? 0 : 1);
        } catch (IOException | InterruptedException | ClassNotFoundException e) {
            e.printStackTrace();
            System.exit(1);
        }
    }

}
