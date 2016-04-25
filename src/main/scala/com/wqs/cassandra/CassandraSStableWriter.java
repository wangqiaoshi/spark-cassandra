package com.wqs.cassandra;

import com.datastax.spark.connector.cql.TableDef;
import com.datastax.spark.connector.writer.DefaultRowWriter;
import com.datastax.spark.connector.writer.RowWriter;
import org.apache.cassandra.io.sstable.CQLSSTableWriter;

import java.io.File;
import java.io.Serializable;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.collection.JavaConverters.*;

/**
 * Created by wqs on 16/4/23.
 */
public class CassandraSStableWriter<T>  implements Serializable{

    private Logger logger = LoggerFactory.getLogger(CassandraSStableWriter.class);


    private File tempFile;
    private TableDef tableDef;
    public CassandraSStableWriter(File tempFile){
        this.tempFile = tempFile;
    }
    public CassandraSStableWriter(TableDef tableDef,File tempFile){
        this.tableDef = tableDef;
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

    public void writeSStable(Iterator<T> datas,String tableSchema, String insertStatement){

        try {
            CQLSSTableWriter writer = CQLSSTableWriter.builder()
                    .inDirectory(tempFile)
                    .forTable(tableSchema)
                    .using(insertStatement)
                    .build();
            RowWriter rowWriter = new DefaultRowWriter(tableDef,tableDef.columnRefs());

        }
        catch (Exception e){
            logger.error(e.getMessage());
        }

    }


}
