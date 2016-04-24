package com.wqs.cassandra;

import org.apache.cassandra.config.EncryptionOptions;
import org.apache.cassandra.thrift.ITransportFactory;
import org.apache.cassandra.thrift.TFramedTransportFactory;

import java.io.File;
import java.net.InetAddress;
import java.util.HashSet;
import java.util.Set;

/**
 * Created by wqs on 16/4/24.
 */
public class LoaderOptions {


    public LoaderOptions(File directory,String[] nodes, int connectionsPerHost)
    {
        try {
            for (String node : nodes) {
                hosts.add(InetAddress.getByName(node.trim()));
            }
            this.directory = directory;
            this.connectionsPerHost =connectionsPerHost;
        }
        catch (Exception e){

        }


    }
    public  File directory;

    public boolean debug;
    public boolean verbose;
    public boolean noProgress;
    public int rpcPort = 9160;
    public String user;
    public String passwd;
    public int throttle = 0;
    public int interDcThrottle = 0;
    public int storagePort;
    public int sslStoragePort;
    public ITransportFactory transportFactory = new TFramedTransportFactory();
    public EncryptionOptions encOptions = new EncryptionOptions.ClientEncryptionOptions();
    public int connectionsPerHost = 1;
    public EncryptionOptions.ServerEncryptionOptions serverEncOptions = new EncryptionOptions.ServerEncryptionOptions();

    public final Set<InetAddress> hosts = new HashSet<>();
    public final Set<InetAddress> ignores = new HashSet<>();
    private  ITransportFactory getTransportFactory(String transportFactory)
    {
        try
        {
            Class<?> factory = Class.forName(transportFactory);
            if (!ITransportFactory.class.isAssignableFrom(factory))
                throw new IllegalArgumentException(String.format("transport factory '%s' " +
                        "not derived from ITransportFactory", transportFactory));
            return (ITransportFactory) factory.newInstance();
        }
        catch (Exception e)
        {
            throw new IllegalArgumentException(String.format("Cannot create a transport factory '%s'.", transportFactory), e);
        }
    }

}
