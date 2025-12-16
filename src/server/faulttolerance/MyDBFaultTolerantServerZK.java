package server.faulttolerance;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Session;
import edu.umass.cs.nio.interfaces.NodeConfig;
import edu.umass.cs.nio.nioutils.NIOHeader;
import edu.umass.cs.nio.nioutils.NodeConfigUtils;
import server.MyDBSingleServer;
import server.ReplicatedServer;

import org.apache.ratis.client.RaftClient;
import org.apache.ratis.client.RaftClientConfigKeys;
import org.apache.ratis.conf.RaftProperties;
import org.apache.ratis.grpc.GrpcConfigKeys;
import org.apache.ratis.protocol.*;
import org.apache.ratis.server.RaftServer;
import org.apache.ratis.server.RaftServerConfigKeys;
import org.apache.ratis.server.storage.RaftStorage;
import org.apache.ratis.statemachine.TransactionContext;
import org.apache.ratis.statemachine.impl.BaseStateMachine;
import org.apache.ratis.proto.RaftProtos;
import org.apache.ratis.util.NetUtils;
import org.apache.ratis.util.TimeDuration;
import org.apache.ratis.util.SizeInBytes;
import org.apache.ratis.thirdparty.com.google.protobuf.ByteString;

import java.io.File;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicLong;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Fault-tolerant replicated DB implementation using Apache Ratis.
 * Implements Raft consensus algorithm for strong consistency and fault tolerance.
 */
public class MyDBFaultTolerantServerZK extends MyDBSingleServer {

    private static final Logger LOGGER = Logger.getLogger(MyDBFaultTolerantServerZK.class.getName());

    public static final int SLEEP = 1000;
    public static final boolean DROP_TABLES_AFTER_TESTS = true;
    public static final int MAX_LOG_SIZE = 400;

    private final String keyspace;
    private final InetSocketAddress cassandraAddress;
    private final NodeConfig<String> nodeConfig;
    private final String myID;

    private Cluster cassandraCluster;
    private Session cassandraSession;

    // Raft components
    private RaftServer raftServer;
    private RaftClient raftClient;
    private final RaftGroupId raftGroupId = RaftGroupId.valueOf(java.util.UUID.fromString("12345678-1234-1234-1234-123456789012"));
    private final ExecutorService executor = Executors.newSingleThreadExecutor();
    private final AtomicLong requestIndex = new AtomicLong(0);

    public MyDBFaultTolerantServerZK(NodeConfig<String> nodeConfig,
                                     String myID,
                                     InetSocketAddress isaDB) throws IOException {
        super(
                new InetSocketAddress(
                        nodeConfig.getNodeAddress(myID),
                        nodeConfig.getNodePort(myID) - ReplicatedServer.SERVER_PORT_OFFSET),
                isaDB,
                myID
        );

        this.keyspace = myID;
        this.cassandraAddress = isaDB;
        this.nodeConfig = nodeConfig;
        this.myID = myID;

        try {
            initCassandra();
            initRaft();
            LOGGER.log(Level.INFO, "MyDBFaultTolerantServerZK with Raft started: {0}", myID);
        } catch (Exception e) {
            LOGGER.log(Level.SEVERE, "Failed to initialize server: {0}", e.getMessage());
            // Continue with basic functionality even if Raft fails
        }
    }

    private void initCassandra() throws IOException {
        try {
            cassandraCluster = Cluster.builder()
                    .addContactPoint(cassandraAddress.getHostString())
                    .withPort(cassandraAddress.getPort())
                    .build();
            cassandraSession = cassandraCluster.connect(keyspace);
            LOGGER.log(Level.INFO, "Connected to Cassandra keyspace {0}", keyspace);
        } catch (Exception e) {
            throw new IOException("Failed to connect to Cassandra", e);
        }
    }

    private void initRaft() throws IOException {
        try {
            // Create Raft properties
            RaftProperties properties = new RaftProperties();

            // Configure storage directory
            File storageDir = new File("ratis-storage/" + myID);
            storageDir.mkdirs();
            RaftServerConfigKeys.setStorageDir(properties, Collections.singletonList(storageDir));

            // Configure network timeouts
            RaftServerConfigKeys.Rpc.setTimeoutMin(properties,
                    org.apache.ratis.util.TimeDuration.valueOf(1000, java.util.concurrent.TimeUnit.MILLISECONDS));
            RaftServerConfigKeys.Rpc.setTimeoutMax(properties,
                    org.apache.ratis.util.TimeDuration.valueOf(3000, java.util.concurrent.TimeUnit.MILLISECONDS));
            RaftClientConfigKeys.Rpc.setRequestTimeout(properties,
                    org.apache.ratis.util.TimeDuration.valueOf(5000, java.util.concurrent.TimeUnit.MILLISECONDS));

            // Configure log settings
            RaftServerConfigKeys.Log.setSegmentSizeMax(properties,
                    org.apache.ratis.util.SizeInBytes.valueOf(1024 * 1024)); // 1MB
            RaftServerConfigKeys.Snapshot.setAutoTriggerEnabled(properties, true);
            RaftServerConfigKeys.Snapshot.setAutoTriggerThreshold(properties, MAX_LOG_SIZE);

            // Build Raft group with all servers
            List<RaftPeer> peers = new ArrayList<>();
            for (String id : nodeConfig.getNodeIDs()) {
                InetSocketAddress addr = new InetSocketAddress(
                        nodeConfig.getNodeAddress(id),
                        nodeConfig.getNodePort(id) + 1000 // Use different port for Raft
                );
                peers.add(RaftPeer.newBuilder()
                        .setId(id)
                        .setAddress(addr)
                        .build());
            }
            RaftGroup raftGroup = RaftGroup.valueOf(raftGroupId, peers);

            // Configure gRPC
            GrpcConfigKeys.Server.setPort(properties,
                    nodeConfig.getNodePort(myID) + 1000);

            // Create and start Raft server
            raftServer = RaftServer.newBuilder()
                    .setGroup(raftGroup)
                    .setProperties(properties)
                    .setServerId(RaftPeerId.valueOf(myID))
                    .setStateMachine(new CassandraStateMachine())
                    .build();

            raftServer.start();

            // Create Raft client
            raftClient = RaftClient.newBuilder()
                    .setProperties(properties)
                    .setRaftGroup(raftGroup)
                    .build();

            LOGGER.log(Level.INFO, "Raft initialized for server {0}", myID);

            // Give some time for leader election to complete
            try {
                Thread.sleep(2000);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }

        } catch (Exception e) {
            throw new IOException("Failed to initialize Raft", e);
        }
    }

    @Override
    protected void handleMessageFromClient(byte[] bytes, NIOHeader header) {
        String command = new String(bytes, StandardCharsets.UTF_8);
        LOGGER.log(Level.INFO, "Received client command: {0}", command);

        // Submit command to Raft for consensus
        if (executor != null && raftClient != null) {
            executor.submit(() -> {
                try {
                    Message message = Message.valueOf(ByteString.copyFrom(command, StandardCharsets.UTF_8));

                    // Reduced retry mechanism for better ordering
                    RaftClientReply reply = null;
                    int maxRetries = 3;
                    int retryCount = 0;

                    while (retryCount < maxRetries) {
                        try {
                            reply = raftClient.io().send(message);
                            if (reply.isSuccess()) {
                                LOGGER.log(Level.INFO, "Raft command succeeded: {0}", command);
                                break;
                            } else {
                                LOGGER.log(Level.WARNING, "Raft command failed (attempt {0}): {1}",
                                        new Object[]{retryCount + 1, reply.getException()});
                            }
                        } catch (Exception e) {
                            LOGGER.log(Level.WARNING, "Raft command exception (attempt {0}): {1}",
                                    new Object[]{retryCount + 1, e.getMessage()});
                        }

                        retryCount++;
                        if (retryCount < maxRetries) {
                            try {
                                Thread.sleep(100);
                            } catch (InterruptedException ie) {
                                Thread.currentThread().interrupt();
                                break;
                            }
                        }
                    }

                    if (reply == null || !reply.isSuccess()) {
                        LOGGER.log(Level.SEVERE, "Failed to execute command after {0} retries: {1}",
                                new Object[]{maxRetries, command});
                    }

                } catch (Exception e) {
                    LOGGER.log(Level.SEVERE, "Error submitting command to Raft: {0}", e.getMessage());
                    e.printStackTrace();
                }
            });
        } else {
            LOGGER.log(Level.WARNING, "Raft not initialized, cannot process command: {0}", command);
        }
    }

    protected void handleMessageFromServer(byte[] bytes, NIOHeader header) {
        // Raft handles inter-server communication internally
        String message = new String(bytes, StandardCharsets.UTF_8);
        LOGGER.log(Level.FINE, "Received server message: {0}", message);
    }

    /**
     * Raft State Machine that applies commands to Cassandra
     */
    private class CassandraStateMachine extends BaseStateMachine {
        private final AtomicLong lastAppliedIndex = new AtomicLong(0);

        @Override
        public CompletableFuture<Message> applyTransaction(TransactionContext trx) {
            RaftProtos.LogEntryProto entry = trx.getLogEntry();
            String command = entry.getStateMachineLogEntry().getLogData().toStringUtf8();

            // Apply commands synchronously to ensure strict ordering
            try {
                if (cassandraSession != null) {
                    LOGGER.log(Level.INFO, "Applying Raft command at index {0}: {1}",
                            new Object[]{entry.getIndex(), command});

                    // Execute the command in the correct keyspace
                    String keyspaceCommand = command.replace("grade", keyspace + ".grade");
                    cassandraSession.execute(keyspaceCommand);
                    lastAppliedIndex.set(entry.getIndex());

                    LOGGER.log(Level.INFO, "Successfully applied Raft command at index {0}: {1}",
                            new Object[]{entry.getIndex(), keyspaceCommand});
                    return CompletableFuture.completedFuture(Message.valueOf(ByteString.copyFromUtf8("SUCCESS")));
                } else {
                    LOGGER.log(Level.SEVERE, "Cassandra session is null, cannot apply command: {0}", command);
                    return CompletableFuture.completedFuture(Message.valueOf(ByteString.copyFromUtf8("ERROR: Cassandra session not available")));
                }
            } catch (Exception e) {
                LOGGER.log(Level.SEVERE, "Error applying Raft command at index {0}: {1}",
                        new Object[]{entry.getIndex(), e.getMessage()});
                e.printStackTrace();
                return CompletableFuture.completedFuture(Message.valueOf(ByteString.copyFromUtf8("ERROR: " + e.getMessage())));
            }
        }

        @Override
        public long takeSnapshot() {
            // Simple snapshot implementation - just return current index
            long index = lastAppliedIndex.get();
            LOGGER.log(Level.INFO, "Taking snapshot at index {0}", index);
            return index;
        }

        @Override
        public void initialize(RaftServer server, RaftGroupId groupId, RaftStorage storage) throws IOException {
            super.initialize(server, groupId, storage);
            LOGGER.log(Level.INFO, "Raft state machine initialized for group {0}", groupId);
        }
    }

    @Override
    public void close() {
        super.close();

        try {
            executor.shutdown();
            if (!executor.awaitTermination(3, TimeUnit.SECONDS)) {
                executor.shutdownNow();
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            executor.shutdownNow();
        }

        try {
            if (raftClient != null) raftClient.close();
        } catch (Exception e) {
            LOGGER.log(Level.WARNING, "Error closing Raft client", e);
        }

        try {
            if (raftServer != null) raftServer.close();
        } catch (Exception e) {
            LOGGER.log(Level.WARNING, "Error closing Raft server", e);
        }

        try {
            if (cassandraSession != null) cassandraSession.close();
        } catch (Exception ignored) {}

        try {
            if (cassandraCluster != null) cassandraCluster.close();
        } catch (Exception ignored) {}
    }

    public static void main(String[] args) throws IOException {
        new MyDBFaultTolerantServerZK(
                NodeConfigUtils.getNodeConfigFromFile(
                        args[0],
                        ReplicatedServer.SERVER_PREFIX,
                        ReplicatedServer.SERVER_PORT_OFFSET),
                args[1],
                args.length > 2
                        ? new InetSocketAddress(
                        args[2].split(":")[0],
                        Integer.parseInt(args[2].split(":")[1]))
                        : new InetSocketAddress("localhost", 9042)
        );
    }
}