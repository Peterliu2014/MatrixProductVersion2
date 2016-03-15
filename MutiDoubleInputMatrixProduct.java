/**
 * Created with IntelliJ IDEA.
 * User: hadoop
 * Date: 16-3-14
 * Time: 下午3:13
 * To change this template use File | Settings | File Templates.
 */
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import java.io.IOException;
import java.lang.reflect.Array;
import java.net.URI;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.util.ReflectionUtils;

public class MutiDoubleInputMatrixProduct {

    public static void initDoubleArrayWritable(int length,DoubleWritable[] doubleArrayWritable){
        for (int i=0;i<length;++i){
          doubleArrayWritable[i]=new DoubleWritable(0.0);
        }
    }

    public static  class MyMapper extends Mapper<IntWritable,DoubleArrayWritable,IntWritable,DoubleArrayWritable>{
        public DoubleArrayWritable map_value=new DoubleArrayWritable();
        public  double[][] leftMatrix=null;/******************************************/
        //public Object obValue=null;
        public DoubleWritable[] arraySum=null;
        public DoubleWritable[] tempColumnArrayDoubleWritable=null;
        public DoubleWritable[] tempRowArrayDoubleWritable=null;
        public double sum=0;
        public double uValue;
        public int leftMatrixRowNum;
        public int leftMatrixColumnNum;
        public void setup(Context context) throws IOException {
            Configuration conf=context.getConfiguration();
            leftMatrixRowNum=conf.getInt("leftMatrixRowNum",10);
            leftMatrixColumnNum=conf.getInt("leftMatrixColumnNum",10);
            leftMatrix=new double[leftMatrixRowNum][leftMatrixColumnNum];
            uValue=(double)(context.getConfiguration().getFloat("u",1.0f));
            tempRowArrayDoubleWritable=new DoubleWritable[leftMatrixColumnNum];
            initDoubleArrayWritable(leftMatrixColumnNum,tempRowArrayDoubleWritable);
            tempColumnArrayDoubleWritable=new DoubleWritable[leftMatrixRowNum];
            initDoubleArrayWritable(leftMatrixRowNum,tempColumnArrayDoubleWritable);
            System.out.println("map setup() start!");
            //URI[] cacheFiles=DistributedCache.getCacheFiles(context.getConfiguration());
            Path[] cacheFiles=DistributedCache.getLocalCacheFiles(conf);
            String localCacheFile="file://"+cacheFiles[0].toString();
            //URI[] cacheFiles=DistributedCache.getCacheFiles(conf);
            //DistributedCache.
            System.out.println("local path is:"+cacheFiles[0].toString());
            // URI[] cacheFiles=DistributedCache.getCacheFiles(context.getConfiguration());
            FileSystem fs =FileSystem.get(URI.create(localCacheFile), conf);
            SequenceFile.Reader reader=null;
            reader=new SequenceFile.Reader(fs,new Path(localCacheFile),conf);
            IntWritable key= (IntWritable)ReflectionUtils.newInstance(reader.getKeyClass(),conf);
            DoubleArrayWritable value= (DoubleArrayWritable)ReflectionUtils.newInstance(reader.getValueClass(),conf);
            //int valueLength=0;
            int rowIndex=0;
            int index;
            while (reader.next(key,value)){
                index=-1;
                for (Writable val:value.get()){
                    tempRowArrayDoubleWritable[++index].set(((DoubleWritable)val).get());
                }
                //obValue=value.toArray();
                rowIndex=key.get();
                leftMatrix[rowIndex]=new double[leftMatrixColumnNum];
                //this.leftMatrix=new double[valueLength][Integer.parseInt(context.getConfiguration().get("leftMatrixColumnNum"))];
                for (int i=0;i<leftMatrixColumnNum;++i){
                    //leftMatrix[rowIndex][i]=Double.parseDouble(Array.get(obValue, i).toString());
                    //leftMatrix[rowIndex][i]=Array.getDouble(obValue, i);
                    leftMatrix[rowIndex][i]= tempRowArrayDoubleWritable[i].get();
                }

            }
            arraySum=new DoubleWritable[leftMatrix.length];
            initDoubleArrayWritable(leftMatrix.length,arraySum);
        }
        public void map(IntWritable key,DoubleArrayWritable value,Context context) throws IOException, InterruptedException {
            //obValue=value.toArray();
            InputSplit inputSplit=context.getInputSplit();
            String fileName=((FileSplit)inputSplit).getPath().getName();
            if (fileName.startsWith("FB")) {
                context.write(key,value);
            }
            else{
                int ii=-1;
                for(Writable val:value.get()){
                    tempColumnArrayDoubleWritable[++ii].set(((DoubleWritable)val).get());
                }
                //arraySum=new DoubleWritable[this.leftMatrix.length];
                for (int i=0;i<this.leftMatrix.length;++i){
                    sum=0;
                    for (int j=0;j<this.leftMatrix[0].length;++j){
                        //sum+= this.leftMatrix[i][j]*Double.parseDouble(Array.get(obValue,j).toString())*(double)(context.getConfiguration().getFloat("u",1f));
                        //sum+= this.leftMatrix[i][j]*Array.getDouble(obValue,j)*uValue;
                        sum+= this.leftMatrix[i][j]*tempColumnArrayDoubleWritable[j].get()*uValue;
                    }
                    arraySum[i].set(sum);
                    //arraySum[i].set(sum);
                }
                map_value.set(arraySum);
                context.write(key,map_value);
            }
        }
    }
    public static class MyReducer extends Reducer<IntWritable,DoubleArrayWritable,IntWritable,DoubleArrayWritable>{
        public DoubleWritable[] sum=null;
       // public Object obValue=null;
        public DoubleArrayWritable valueArrayWritable=new DoubleArrayWritable();
        public DoubleWritable[] tempColumnArrayDoubleWritable=null;
       // public DoubleWritable[] tempRowArrayDoubleWritable=null;
        //private int leftMatrixColumnNum;
        private int leftMatrixRowNum;

        public void setup(Context context){
            //leftMatrixColumnNum=context.getConfiguration().getInt("leftMatrixColumnNum",100);
            leftMatrixRowNum=context.getConfiguration().getInt("leftMatrixRowNum",100);
            sum=new DoubleWritable[leftMatrixRowNum];
            initDoubleArrayWritable(leftMatrixRowNum,sum);
            //tempRowArrayDoubleWritable=new DoubleWritable[leftMatrixColumnNum];
            tempColumnArrayDoubleWritable=new DoubleWritable[leftMatrixRowNum];
            initDoubleArrayWritable(leftMatrixRowNum,tempColumnArrayDoubleWritable);
        }

        public void reduce(IntWritable key,Iterable<DoubleArrayWritable>value,Context context) throws IOException, InterruptedException {
            //int valueLength=0;
            for(DoubleArrayWritable doubleValue:value){
                int index=-1;
                for (Writable val:doubleValue.get()){
                    tempColumnArrayDoubleWritable[++index].set(((DoubleWritable)val).get());
                 }
                //valueLength=Array.getLength(obValue);
                for (int i=0;i<leftMatrixRowNum;++i){
                    //sum[i]=new DoubleWritable(Double.parseDouble(Array.get(obValue,i).toString())+sum[i].get());
                    //sum[i]=new DoubleWritable(Array.getDouble(obValue,i)+sum[i].get());
                    sum[i].set(tempColumnArrayDoubleWritable[i].get()+sum[i].get());
                }
            }
            //valueArrayWritable.set(sum);
            valueArrayWritable.set(tempColumnArrayDoubleWritable);
            context.write(key,valueArrayWritable);
            for (int i=0;i<sum.length;++i){
                sum[i].set(0.0);
            }

        }
    }

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        String uri=args[3];
        String outUri=args[4];
        String cachePath=args[2];
        HDFSOperator.deleteDir(outUri);
        Configuration conf=new Configuration();
        DistributedCache.addCacheFile(URI.create(cachePath),conf);//添加分布式缓存
        /**************************************************/
        //FileSystem fs=FileSystem.get(URI.create(uri),conf);
        //fs.delete(new Path(outUri),true);
        /*********************************************************/
        conf.setInt("leftMatrixColumnNum",Integer.parseInt(args[0]));
        conf.setInt("leftMatrixRowNum",Integer.parseInt(args[1]));
        conf.setFloat("u",0.35f);
        conf.set("mapred.jar","MutiDoubleInputMatrixProduct.jar");
        Job job=new Job(conf,"MatrixProdcut");
        job.setJarByClass(MutiDoubleInputMatrixProduct.class);
        job.setInputFormatClass(SequenceFileInputFormat.class);
        job.setOutputFormatClass(SequenceFileOutputFormat.class);
        job.setMapperClass(MyMapper.class);
        job.setReducerClass(MyReducer.class);
        job.setMapOutputKeyClass(IntWritable.class);
        job.setMapOutputValueClass(DoubleArrayWritable.class);
        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(DoubleArrayWritable.class);
        FileInputFormat.setInputPaths(job, new Path(uri));
        FileOutputFormat.setOutputPath(job,new Path(outUri));
        System.exit(job.waitForCompletion(true)?0:1);
    }


}
class DoubleArrayWritable extends ArrayWritable {
    public DoubleArrayWritable(){
        super(DoubleWritable.class);
    }
/*
    public String toString(){
        StringBuilder sb=new StringBuilder();
        for (Writable val:get()){
            DoubleWritable doubleWritable=(DoubleWritable)val;
            sb.append(doubleWritable.get());
            sb.append(",");
        }
        sb.deleteCharAt(sb.length()-1);
        return sb.toString();
    }
*/
}

class HDFSOperator{
    public static boolean deleteDir(String dir)throws IOException{
        Configuration conf=new Configuration();
        FileSystem fs =FileSystem.get(conf);
        boolean result=fs.delete(new Path(dir),true);
        System.out.println("sOutput delete");
        fs.close();
        return result;
    }
}
