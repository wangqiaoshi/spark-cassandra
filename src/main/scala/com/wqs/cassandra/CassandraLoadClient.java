package com.wqs.cassandra;




import org.apache.cassandra.tools.BulkLoadConnectionFactory;
import org.apache.commons.lang3.StringUtils;

import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TTransport;

import org.apache.cassandra.auth.IAuthenticator;
import org.apache.cassandra.config.*;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.db.SystemKeyspace;
import org.apache.cassandra.db.marshal.UTF8Type;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.io.sstable.SSTableLoader;
import org.apache.cassandra.streaming.*;
import org.apache.cassandra.thrift.*;
import org.apache.cassandra.utils.ByteBufferUtil;

import java.io.Serializable;
import java.net.InetAddress;
import java.util.*;

/**
 * Created by wqs on 16/4/23.
 */
public class CassandraLoadClient extends SSTableLoader.Client implements Serializable {
    private final Map<String, CFMetaData> knownCfs = new HashMap<>();
    private final Set<InetAddress> hosts;
    private final int rpcPort;
    private final String user;
    private final String passwd;
    private final ITransportFactory transportFactory;
    private final int storagePort;
    private final int sslStoragePort;
    private final EncryptionOptions.ServerEncryptionOptions serverEncOptions;

    public CassandraLoadClient(LoaderOptions options)
    {
        super();
        this.hosts = options.hosts;
        this.rpcPort = options.rpcPort;
        this.user = options.user;
        this.passwd = options.passwd;
        this.transportFactory = options.transportFactory;
        this.storagePort = options.storagePort;
        this.sslStoragePort = options.sslStoragePort;
        this.serverEncOptions = options.serverEncOptions;
    }

    @Override
    public void init(String keyspace)
    {
        Iterator<InetAddress> hostiter = hosts.iterator();
        while (hostiter.hasNext())
        {
            try
            {
                // Query endpoint to ranges map and schemas from thrift
                InetAddress host = hostiter.next();
                Cassandra.Client client = createThriftClient(host.getHostAddress(), rpcPort, this.user, this.passwd, this.transportFactory);

                setPartitioner(client.describe_partitioner());
                Token.TokenFactory tkFactory = getPartitioner().getTokenFactory();

                for (TokenRange tr : client.describe_ring(keyspace))
                {
                    Range<Token> range = new Range<>(tkFactory.fromString(tr.start_token), tkFactory.fromString(tr.end_token), getPartitioner());
                    for (String ep : tr.endpoints)
                    {
                        addRangeForEndpoint(range, InetAddress.getByName(ep));
                    }
                }

                String cfQuery = String.format("SELECT %s FROM %s.%s WHERE keyspace_name = '%s'",
                        StringUtils.join(getCFColumnsWithoutCollections(), ","),
                        Keyspace.SYSTEM_KS,
                        SystemKeyspace.SCHEMA_COLUMNFAMILIES_CF,
                        keyspace);
                CqlResult cfRes = client.execute_cql3_query(ByteBufferUtil.bytes(cfQuery), Compression.NONE, ConsistencyLevel.ONE);


                for (CqlRow row : cfRes.rows)
                {
                    String columnFamily = UTF8Type.instance.getString(row.columns.get(1).bufferForName());
                    String columnsQuery = String.format("SELECT * FROM %s.%s WHERE keyspace_name = '%s' AND columnfamily_name = '%s'",
                            Keyspace.SYSTEM_KS,
                            SystemKeyspace.SCHEMA_COLUMNS_CF,
                            keyspace,
                            columnFamily);
                    CqlResult columnsRes = client.execute_cql3_query(ByteBufferUtil.bytes(columnsQuery), Compression.NONE, ConsistencyLevel.ONE);

                    CFMetaData metadata = CFMetaData.fromThriftCqlRow(row, columnsRes);
                    knownCfs.put(metadata.cfName, metadata);
                }
                break;
            }
            catch (Exception e)
            {
                if (!hostiter.hasNext())
                    throw new RuntimeException("Could not retrieve endpoint ranges: ", e);
            }
        }
    }

    //Remove dropped_columns since we can't parse collections in v2 which is used by thrift
    //See CASSANDRA-10700
    List<String> getCFColumnsWithoutCollections()
    {

        Iterator<ColumnDefinition> allColumns = CFMetaData.SchemaColumnFamiliesCf.allColumnsInSelectOrder();
        List<String> selectedColumns = new ArrayList<>();

        while (allColumns.hasNext())
        {
            ColumnDefinition def = allColumns.next();

            if (!def.type.isCollection())
                selectedColumns.add(UTF8Type.instance.getString(def.name.bytes));
        }

        return selectedColumns;
    }

    @Override
    public StreamConnectionFactory getConnectionFactory()
    {
        return new BulkLoadConnectionFactory(storagePort, sslStoragePort, serverEncOptions, false);
    }

    @Override
    public CFMetaData getCFMetaData(String keyspace, String cfName)
    {
        return knownCfs.get(cfName);
    }

    private static Cassandra.Client createThriftClient(String host, int port, String user, String passwd, ITransportFactory transportFactory) throws Exception
    {
        TTransport trans = transportFactory.openTransport(host, port);
        TProtocol protocol = new TBinaryProtocol(trans);
        Cassandra.Client client = new Cassandra.Client(protocol);
        if (user != null && passwd != null)
        {
            Map<String, String> credentials = new HashMap<>();
            credentials.put(IAuthenticator.USERNAME_KEY, user);
            credentials.put(IAuthenticator.PASSWORD_KEY, passwd);
            AuthenticationRequest authenticationRequest = new AuthenticationRequest(credentials);
            client.login(authenticationRequest);
        }
        return client;
    }
}
