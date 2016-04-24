package com.wqs.cassandra;

import java.io.File;
import java.io.FilenameFilter;
import java.io.IOException;
import java.net.InetAddress;
import java.util.*;

import com.google.common.collect.HashMultimap;
import com.google.common.collect.Multimap;

import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.config.Config;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.dht.IPartitioner;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.io.sstable.Component;
import org.apache.cassandra.io.sstable.Descriptor;
import org.apache.cassandra.io.sstable.SSTable;
import org.apache.cassandra.io.sstable.SSTableLoader.Client;
import org.apache.cassandra.io.sstable.SSTableReader;
import org.apache.cassandra.service.ActiveRepairService;
import org.apache.cassandra.streaming.*;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.OutputHandler;
import org.apache.cassandra.utils.Pair;

import org.apache.cassandra.utils.concurrent.Ref;

/**
 * Created by wqs on 16/4/24.
 */
public class CassandraLoader implements StreamEventHandler {

    private final File directory;
    private final String keyspace;
    private final Client client;
    private final int connectionsPerHost;
    private final OutputHandler outputHandler;
    private final Set<InetAddress> failedHosts = new HashSet<>();

    private final List<SSTableReader> sstables = new ArrayList<>();
    private final Multimap<InetAddress, StreamSession.SSTableStreamingSections> streamingDetails = HashMultimap.create();

    public CassandraLoader(File directory, Client client, OutputHandler outputHandler)
    {
        this(directory, client, outputHandler, 1);
    }

    public CassandraLoader(File directory, Client client, OutputHandler outputHandler, int connectionsPerHost)
    {
        this.directory = directory;
        this.keyspace = directory.getParentFile().getName();
        this.client = client;
        this.outputHandler = outputHandler;
        this.connectionsPerHost = connectionsPerHost;
    }

    protected Collection<SSTableReader> openSSTables(final Map<InetAddress, Collection<Range<Token>>> ranges)
    {
        outputHandler.output("Opening sstables and calculating sections to stream");

        directory.list(new FilenameFilter()
        {
            public boolean accept(File dir, String name)
            {
                if (new File(dir, name).isDirectory())
                    return false;
                Pair<Descriptor, Component> p = SSTable.tryComponentFromFilename(dir, name);
                Descriptor desc = p == null ? null : p.left;
                if (p == null || !p.right.equals(Component.DATA) || desc.type.isTemporary)
                    return false;

                if (!new File(desc.filenameFor(Component.PRIMARY_INDEX)).exists())
                {
                    outputHandler.output(String.format("Skipping file %s because index is missing", name));
                    return false;
                }

                CFMetaData metadata = client.getCFMetaData(keyspace, desc.cfname);
                if (metadata == null)
                {
                    outputHandler.output(String.format("Skipping file %s: column family %s.%s doesn't exist", name, keyspace, desc.cfname));
                    return false;
                }

                Set<Component> components = new HashSet<>();
                components.add(Component.DATA);
                components.add(Component.PRIMARY_INDEX);
                if (new File(desc.filenameFor(Component.SUMMARY)).exists())
                    components.add(Component.SUMMARY);
                if (new File(desc.filenameFor(Component.COMPRESSION_INFO)).exists())
                    components.add(Component.COMPRESSION_INFO);
                if (new File(desc.filenameFor(Component.STATS)).exists())
                    components.add(Component.STATS);

                try
                {
                    // To conserve memory, open SSTableReaders without bloom filters and discard
                    // the index summary after calculating the file sections to stream and the estimated
                    // number of keys for each endpoint. See CASSANDRA-5555 for details.
                    SSTableReader sstable = SSTableReader.openForBatch(desc, components, metadata, client.getPartitioner());
                    sstables.add(sstable);

                    // calculate the sstable sections to stream as well as the estimated number of
                    // keys per host
                    for (Map.Entry<InetAddress, Collection<Range<Token>>> entry : ranges.entrySet())
                    {
                        InetAddress endpoint = entry.getKey();
                        Collection<Range<Token>> tokenRanges = entry.getValue();

                        List<Pair<Long, Long>> sstableSections = sstable.getPositionsForRanges(tokenRanges);
                        long estimatedKeys = sstable.estimatedKeysForRanges(tokenRanges);
                        Ref ref = sstable.tryRef();
                        if (ref == null)
                            throw new IllegalStateException("Could not acquire ref for "+sstable);
                        StreamSession.SSTableStreamingSections details = new StreamSession.SSTableStreamingSections(ref, sstableSections, estimatedKeys, ActiveRepairService.UNREPAIRED_SSTABLE);
                        streamingDetails.put(endpoint, details);
                    }

                    // to conserve heap space when bulk loading
                    sstable.releaseSummary();
                }
                catch (IOException e)
                {
                    outputHandler.output(String.format("Skipping file %s, error opening it: %s", name, e.getMessage()));
                }
                return false;
            }
        });
        return sstables;
    }

    public StreamResultFuture stream()
    {
        return stream(Collections.<InetAddress>emptySet());
    }

    public StreamResultFuture stream(Set<InetAddress> toIgnore, StreamEventHandler... listeners)
    {
        client.init(keyspace);
        outputHandler.output("Established connection to initial hosts");

        StreamPlan plan = new StreamPlan("Bulk Load", 0, connectionsPerHost).connectionFactory(client.getConnectionFactory());

        Map<InetAddress, Collection<Range<Token>>> endpointToRanges = client.getEndpointToRangesMap();
        openSSTables(endpointToRanges);
        if (sstables.isEmpty())
        {
            // return empty result
            return plan.execute();
        }

        outputHandler.output(String.format("Streaming relevant part of %sto %s", names(sstables), endpointToRanges.keySet()));

        for (Map.Entry<InetAddress, Collection<Range<Token>>> entry : endpointToRanges.entrySet())
        {
            InetAddress remote = entry.getKey();
            if (toIgnore.contains(remote))
                continue;

            List<StreamSession.SSTableStreamingSections> endpointDetails = new LinkedList<>();

            // references are acquired when constructing the SSTableStreamingSections above
            for (StreamSession.SSTableStreamingSections details : streamingDetails.get(remote))
            {
                endpointDetails.add(details);
            }

            plan.transferFiles(remote, endpointDetails);
        }
        plan.listeners(this, listeners);
        return plan.execute();
    }

    public void onSuccess(StreamState finalState)
    {
        releaseReferences();
    }
    public void onFailure(Throwable t)
    {
        releaseReferences();
    }

    /**
     * releases the shared reference for all sstables, we acquire this when opening the sstable
     */
    private void releaseReferences()
    {
        for (SSTableReader sstable : sstables)
        {
            sstable.selfRef().release();
            assert sstable.selfRef().globalCount() == 0;
        }
    }

    public void handleStreamEvent(StreamEvent event)
    {
        if (event.eventType == StreamEvent.Type.STREAM_COMPLETE)
        {
            StreamEvent.SessionCompleteEvent se = (StreamEvent.SessionCompleteEvent) event;
            if (!se.success)
                failedHosts.add(se.peer);
        }
    }

    private String names(Collection<SSTableReader> sstables)
    {
        StringBuilder builder = new StringBuilder();
        for (SSTableReader sstable : sstables)
            builder.append(sstable.descriptor.filenameFor(Component.DATA)).append(" ");
        return builder.toString();
    }

    public Set<InetAddress> getFailedHosts()
    {
        return failedHosts;
    }

}