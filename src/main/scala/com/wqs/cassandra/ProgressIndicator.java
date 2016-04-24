package com.wqs.cassandra;

import com.google.common.collect.HashMultimap;
import com.google.common.collect.Multimap;
import org.apache.cassandra.streaming.*;

import java.io.Serializable;
import java.net.InetAddress;

/**
 * Created by wqs on 16/4/23.
 */
public class ProgressIndicator implements StreamEventHandler,Serializable {

    private long start;
    private long lastProgress;
    private long lastTime;

    private int peak = 0;
    private int totalFiles = 0;

    private final Multimap<InetAddress, SessionInfo> sessionsByHost = HashMultimap.create();

    public ProgressIndicator()
    {
        start = lastTime = System.nanoTime();
    }

    public void onSuccess(StreamState finalState) {}
    public void onFailure(Throwable t) {}


    public synchronized void handleStreamEvent(StreamEvent event)
    {
        if (event.eventType == StreamEvent.Type.STREAM_PREPARED)
        {
            SessionInfo session = ((StreamEvent.SessionPreparedEvent) event).session;
            sessionsByHost.put(session.peer, session);
        }
        else if (event.eventType == StreamEvent.Type.FILE_PROGRESS || event.eventType == StreamEvent.Type.STREAM_COMPLETE)
        {
            ProgressInfo progressInfo = null;
            if (event.eventType == StreamEvent.Type.FILE_PROGRESS)
            {
                progressInfo = ((StreamEvent.ProgressEvent) event).progress;
            }

            long time = System.nanoTime();
            long deltaTime = time - lastTime;

            StringBuilder sb = new StringBuilder();
            sb.append("\rprogress: ");

            long totalProgress = 0;
            long totalSize = 0;

            boolean updateTotalFiles = totalFiles == 0;
            // recalculate progress across all sessions in all hosts and display
            for (InetAddress peer : sessionsByHost.keySet())
            {
                sb.append("[").append(peer.toString()).append("]");

                for (SessionInfo session : sessionsByHost.get(peer))
                {
                    long size = session.getTotalSizeToSend();
                    long current = 0;
                    int completed = 0;

                    if (progressInfo != null && session.peer.equals(progressInfo.peer) && (session.sessionIndex == progressInfo.sessionIndex))
                    {
                        session.updateProgress(progressInfo);
                    }
                    for (ProgressInfo progress : session.getSendingFiles())
                    {
                        if (progress.isCompleted())
                            completed++;
                        current += progress.currentBytes;
                    }
                    totalProgress += current;

                    totalSize += size;

                    sb.append(session.sessionIndex).append(":");
                    sb.append(completed).append("/").append(session.getTotalFilesToSend());
                    sb.append(" ").append(String.format("%-3d", size == 0 ? 100L : current * 100L / size)).append("% ");

                    if (updateTotalFiles)
                        totalFiles += session.getTotalFilesToSend();
                }
            }

            lastTime = time;
            long deltaProgress = totalProgress - lastProgress;
            lastProgress = totalProgress;

            sb.append("total: ").append(totalSize == 0 ? 100L : totalProgress * 100L / totalSize).append("% ");
            sb.append(String.format("%-3d", mbPerSec(deltaProgress, deltaTime))).append("MB/s");
            int average = mbPerSec(totalProgress, (time - start));
            if (average > peak)
                peak = average;
            sb.append("(avg: ").append(average).append(" MB/s)");

            System.out.print(sb.toString());
        }
    }

    private int mbPerSec(long bytes, long timeInNano)
    {
        double bytesPerNano = ((double)bytes) / timeInNano;
        return (int)((bytesPerNano * 1000 * 1000 * 1000) / (1024 * 1024));
    }

    public void printSummary(int connectionsPerHost)
    {
        long end = System.nanoTime();
        long durationMS = ((end - start) / (1000000));
        int average = mbPerSec(lastProgress, (end - start));
        StringBuilder sb = new StringBuilder();
        sb.append("\nSummary statistics: \n");
        sb.append(String.format("   %-30s: %-10d%n", "Connections per host: ", connectionsPerHost));
        sb.append(String.format("   %-30s: %-10d%n", "Total files transferred: ", totalFiles));
        sb.append(String.format("   %-30s: %-10d%n", "Total bytes transferred: ", lastProgress));
        sb.append(String.format("   %-30s: %-10d%n", "Total duration (ms): ", durationMS));
        sb.append(String.format("   %-30s: %-10d%n", "Average transfer rate (MB/s): ", + average));
        sb.append(String.format("   %-30s: %-10d%n", "Peak transfer rate (MB/s): ", + peak));
        System.out.println(sb.toString());
    }
}
