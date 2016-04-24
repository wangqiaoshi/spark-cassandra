package com.wqs.cassandra;

import org.apache.cassandra.io.sstable.CQLSSTableWriter;

import java.io.File;
import java.io.Serializable;
import java.util.List;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Created by wqs on 16/4/23.
 */
public class CassandraSStableWriter  implements Serializable{

    private Logger logger = LoggerFactory.getLogger(CassandraSStableWriter.class);


    private File tempFile;
    public CassandraSStableWriter(File tempFile){
        this.tempFile = tempFile;
    }


    public void writeSStable(List<Map<String,Object>> datas, String tableSchema, String insertStatement){
        try {
        CQLSSTableWriter writer = CQLSSTableWriter.builder()
                .inDirectory(tempFile)
                .forTable(tableSchema)
                .using(insertStatement)
                .build();
            for(Map<String,Object> data:datas){
                writer.addRow(data);
            }
            writer.close();
        }catch (Exception e){
            logger.error(e.getMessage());

        }

    }


}
