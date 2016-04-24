package com.wqs.cassandra;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.streaming.StreamResultFuture;
import org.apache.cassandra.utils.OutputHandler;

import java.io.File;
import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;


/**
 * Created by wqs on 16/4/24.
 */
public   class Application {

    public static void main(String[] args) {
        try{
        File tempFile = getSStableTempDir("myks","table3");
        CassandraSStableWriter writer = new CassandraSStableWriter(tempFile);

        String tableScheme = "create table myks.table3(uuid bigint primary key,value text)";
        String insertStatement = "insert into myks.table1(uuid,value) values(?,?)";
        Map<String,Object> map1= new HashMap<>();
        map1.put("uuid",100l);
        map1.put("value","v1");
        Map<String,Object> map2= new HashMap<>();
        map2.put("uuid",101l);
        map2.put("value","v2");

        List<Map<String,Object>> datas = new ArrayList<>();
        datas.add(map1);
        datas.add(map2);
        writer.writeSStable(datas,tableScheme,insertStatement);
            loadSStable(tempFile);

        }catch (Exception e){
            e.printStackTrace();
        }
    }


    public  static File getSStableTempDir(String keySpace,String tableName) throws Exception{
        int maxAttempt=10;
        int currentAttempt=1;
        File tempFile =null;
        String rootTempDir = System.getProperty("java.io.tmpdir");
        while (tempFile==null){
            if(currentAttempt>maxAttempt){
                throw  new Exception("create file failed");
            }
            String path= "spark-"+ System.currentTimeMillis()+File.separator+keySpace+File.separator+tableName;
            tempFile = new File(rootTempDir+path);
            System.out.println(rootTempDir+path);
            if(!tempFile.mkdirs())
                tempFile=null;
            currentAttempt+=1;
        }
        return tempFile;

    }


    public static void loadSStable(File tempFile){
        try {
            String[] nodes = {"192.168.6.52"};
            LoaderOptions options = new LoaderOptions(tempFile, nodes, 5);
            OutputHandler handler = new OutputHandler.SystemOutput(options.verbose, options.debug);
            CassandraLoadClient client = new CassandraLoadClient(options);
            CassandraLoader loader = new CassandraLoader(tempFile, client, handler, options.connectionsPerHost);
            DatabaseDescriptor.setStreamThroughputOutboundMegabitsPerSec(options.throttle);
            DatabaseDescriptor.setInterDCStreamThroughputOutboundMegabitsPerSec(options.interDcThrottle);
            ProgressIndicator indicator = new ProgressIndicator();
            StreamResultFuture future = loader.stream(options.ignores, indicator);
            indicator.printSummary(options.connectionsPerHost);
            future.get();
        }
        catch (Exception e){

        }



    }


}

