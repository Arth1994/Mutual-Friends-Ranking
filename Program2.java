import java.io.*;
import java.util.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

//Arth Shah Program2
//Implemented Using Job Chaining 2 Mapper and 2 Reducer

public class Program2
{

	//Mapper 1
    public static class Map1 extends Mapper<LongWritable, Text, Text, Text>
    {

        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

            String[] usr = value.toString().split("\t");

            if (usr.length != 2)
            {
                return;
            }
            else
                {

                String usermain = usr[0];


                List<String> userFriends = Arrays.asList(usr[1].split(","));

                for (String currFriend : userFriends) {

                    String userkey;

                    if (Integer.parseInt(usermain) < Integer.parseInt(currFriend))
                    {
                        userkey = usermain + "\t" + currFriend;
                    }
                    else
                    {
                        userkey = currFriend + "\t" + usermain;
                    }

                    context.write(new Text(userkey), new Text(usr[1]));
                }
            }
        }
    }

    /*
    *
    *     public static class Reducer1 extends Reducer<Text, Text, Text, Text>
    {

        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

            HashMap<String, Integer> friends_map = new HashMap<String, Integer>();


            int count = 0;

            for (Text friendList : values)
            {

                List<String> friends = Arrays.asList(friendList.toString().split(","));
                for (String friend : friends)
                {
                    if (friends_map.containsKey(friend))
                    {
                        count++;
                    }
                    else
                    {
                        friends_map.put(friend, 1);
                    }

                }
            }
            Integer result = (Integer)count;
            context.write(key,new Text(result.toString()));
        }
    }
     */



    public static class Reducer1 extends Reducer<Text, Text, Text, Text>
    {

        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

            HashMap<String, Integer> friends_map = new HashMap<String, Integer>();


            int count = 0;

            for (Text friendList : values)
            {

                List<String> friends = Arrays.asList(friendList.toString().split(","));
                for (String friend : friends)
                {
                    if (friends_map.containsKey(friend))
                    {
                        count++;
                    }
                    else
                    {
                        friends_map.put(friend, 1);
                    }

                }
            }
            Integer result = (Integer)count;
            context.write(key,new Text(result.toString()));
        }
    }

    public static class Map2 extends Mapper<LongWritable, Text, MyWritaCompare, Text>
    {

        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException
        {
            {
                String[] user = value.toString().split("\t");
                String resultkey1 = user[0]+"\t"+user[1];

                Integer result = Integer.parseInt(user[2].toString());
                Text resultkey = new Text(resultkey1.toString());
                context.write(new MyWritaCompare(resultkey, result), new Text(user[2]));
            }
        }
    }

    public static class Reducer2 extends Reducer<MyWritaCompare, Text, Text, Text> {

        int stpcount = 1;
        public  void reduce(MyWritaCompare key, Iterable<Text> values,Context context) throws IOException, InterruptedException {

            int count = 0;


            for (Text count1 : values)
            {

                if(stpcount <=  10)
                {
                    Integer countint = Integer.parseInt(count1.toString());
                    count = (int) countint;

                    Integer countmain = new Integer(count);
                    context.write(new Text(key.toString()),new Text(countmain.toString()));
                }

                else{

                    break;

                }
                stpcount++;
            }



        }
    }


    //Driver Program Main Method
    public static void main(String[] args) throws Exception{


        Configuration conf = new Configuration();
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();

        if (otherArgs.length != 3) {
            System.err.println("Usage: Program2 <Input Path> <Output  Path1> <Output final path>");
            System.exit(2);
        }

        Job job = new Job(conf, "Program2 Program1");
        job.setJarByClass(Program2.class);

        job.setInputFormatClass(TextInputFormat.class);


        job.setMapperClass(Map1.class);
        job.setReducerClass(Reducer1.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
        FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
        boolean mapreduce = job.waitForCompletion(true);

        if (mapreduce) {
            Configuration conf1 = new Configuration();

            Job job1 = new Job(conf1, "Program2 Program2");
            job1.setJarByClass(Program2.class);

            job1.setMapperClass(Map2.class);
            job1.setReducerClass(Reducer2.class);

            job1.setInputFormatClass(TextInputFormat.class);


            job1.setMapOutputKeyClass(MyWritaCompare.class);
            job1.setMapOutputValueClass(Text.class);

            job1.setOutputKeyClass(Text.class);
            job1.setGroupingComparatorClass(CustomComparator.class);
            job1.setSortComparatorClass(MyComparator.class);

            job1.setOutputValueClass(IntWritable.class);

            FileInputFormat.addInputPath(job1, new Path(otherArgs[1]));

            FileOutputFormat.setOutputPath(job1, new Path(otherArgs[2]));

            System.exit(job1.waitForCompletion(true) ? 0 : 1);

        }



    }
}

class MyWritaCompare implements WritableComparable<MyWritaCompare>
{

    String key_val;
    Integer count;

    public String getKey() {
        return key_val;
    }

    public void setKey(String keyvalue) {
        this.key_val = keyvalue;
    }

    public Integer getCount() {
        return count;
    }

    public void setCount(Integer count) {
        this.count = count;
    }


    MyWritaCompare(Text key1, Integer count) {
        this.key_val = key1.toString();
        this.count = count;
    }

    MyWritaCompare() {
    }


    @Override
    public void readFields(DataInput arg0) throws IOException {
        // TODO Auto-generated method stub

        key_val = arg0.readUTF();
        count = arg0.readInt();

    }

    @Override
    public void write(DataOutput arg0) throws IOException {
        // TODO Auto-generated method stub
        arg0.writeUTF(key_val);
        arg0.writeInt(count);


    }

    @Override
    public String toString() {
        return key_val.toString();
    }

    @Override
    public int compareTo(MyWritaCompare t) {
        // TODO Auto-generated method stub
        String[] keys = this.toString().split("\t");
        String key1 = keys[0];
        String key2 = keys[1];

        String[] tkeys = t.toString().split("\t");
        String tkey1 = tkeys[0];
        String tkey2 = tkeys[1];

        Integer thiskey1 = Integer.parseInt(key1);
        Integer thatkey1 = Integer.parseInt(tkey1);

        Integer thiskey2 = Integer.parseInt(key2);
        Integer thatkey2 = Integer.parseInt(tkey2);


        Integer thiskey3 = this.getCount();
        Integer thatkey3 = t.getCount();


        return thiskey3.compareTo(thatkey3) != 0 ? thatkey3.compareTo(thiskey3)
                : (thiskey1 > thatkey1 ? 1 : (thiskey1 == thatkey1
                ? (thiskey2 > thatkey2 ? -1 : (thiskey2 == thatkey2
                ? 0 : 1)) : -1));


    }


}


class CustomComparator extends WritableComparator {

    protected CustomComparator() {
        super(MyWritaCompare.class, true);
    }

    @Override
    public int compare(WritableComparable a, WritableComparable b) {

        MyWritaCompare a_key = (MyWritaCompare) a;
        MyWritaCompare b_key = (MyWritaCompare) b;

        Integer thiskey = a_key.getCount();
        Integer thatkey = b_key.getCount();

        return thiskey.compareTo(thatkey);
    }
}

class MyComparator extends WritableComparator {

    protected MyComparator() {
        super(MyWritaCompare.class, true);
    }

    @Override
    public int compare( WritableComparable a, WritableComparable b) {
        MyWritaCompare a_key = (MyWritaCompare) a;
        MyWritaCompare b_key = (MyWritaCompare) b;

        String[] keys = a_key.toString().split("\t");
        String key1 = keys[0];
        String key2 = keys[1];

        String[] tkeys = b_key.toString().split("\t");
        String tkey1 = tkeys[0];
        String tkey2 = tkeys[1];

        Integer thiskey1 = Integer.parseInt(key1);
        Integer thatkey1 = Integer.parseInt(tkey1);

        Integer thiskey2 = Integer.parseInt(key2);
        Integer thatkey2 = Integer.parseInt(tkey2);


        Integer thiskey3 = a_key.getCount();
        Integer thatkey3 = b_key.getCount();




        return thiskey3.compareTo(thatkey3) != 0 ? thatkey3.compareTo(thiskey3)
                : (thiskey1 > thatkey1 ? 1 : (thiskey1 == thatkey1
                ? (thiskey2 > thatkey2 ? -1 : (thiskey2 == thatkey2
                ? 0 : 1)) : -1));

    }

}
