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
    public int storagePort=7000;
    public int sslStoragePort=7001;
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

    public File getDirectory() {
        return directory;
    }

    public void setDirectory(File directory) {
        this.directory = directory;
    }

    public boolean isDebug() {
        return debug;
    }

    public void setDebug(boolean debug) {
        this.debug = debug;
    }

    public boolean isVerbose() {
        return verbose;
    }

    public void setVerbose(boolean verbose) {
        this.verbose = verbose;
    }

    public boolean isNoProgress() {
        return noProgress;
    }

    public void setNoProgress(boolean noProgress) {
        this.noProgress = noProgress;
    }

    public int getRpcPort() {
        return rpcPort;
    }

    public void setRpcPort(int rpcPort) {
        this.rpcPort = rpcPort;
    }

    public String getUser() {
        return user;
    }

    public void setUser(String user) {
        this.user = user;
    }

    public String getPasswd() {
        return passwd;
    }

    public void setPasswd(String passwd) {
        this.passwd = passwd;
    }

    public int getThrottle() {
        return throttle;
    }

    public void setThrottle(int throttle) {
        this.throttle = throttle;
    }

    public int getInterDcThrottle() {
        return interDcThrottle;
    }

    public void setInterDcThrottle(int interDcThrottle) {
        this.interDcThrottle = interDcThrottle;
    }

    public int getStoragePort() {
        return storagePort;
    }

    public void setStoragePort(int storagePort) {
        this.storagePort = storagePort;
    }

    public int getSslStoragePort() {
        return sslStoragePort;
    }

    public void setSslStoragePort(int sslStoragePort) {
        this.sslStoragePort = sslStoragePort;
    }

    public ITransportFactory getTransportFactory() {
        return transportFactory;
    }

    public void setTransportFactory(ITransportFactory transportFactory) {
        this.transportFactory = transportFactory;
    }

    public EncryptionOptions getEncOptions() {
        return encOptions;
    }

    public void setEncOptions(EncryptionOptions encOptions) {
        this.encOptions = encOptions;
    }

    public int getConnectionsPerHost() {
        return connectionsPerHost;
    }

    public void setConnectionsPerHost(int connectionsPerHost) {
        this.connectionsPerHost = connectionsPerHost;
    }

    public EncryptionOptions.ServerEncryptionOptions getServerEncOptions() {
        return serverEncOptions;
    }

    public void setServerEncOptions(EncryptionOptions.ServerEncryptionOptions serverEncOptions) {
        this.serverEncOptions = serverEncOptions;
    }

    public Set<InetAddress> getHosts() {
        return hosts;
    }

    public Set<InetAddress> getIgnores() {
        return ignores;
    }
}
