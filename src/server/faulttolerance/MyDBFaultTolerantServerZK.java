//package server.faulttolerance;
//
//import edu.umass.cs.nio.nioutils.NIOHeader;
//import edu.umass.cs.nio.nioutils.NodeConfigUtils;
//import edu.umass.cs.nio.interfaces.NodeConfig;
//import edu.umass.cs.utils.Util;
//import server.MyDBSingleServer;
//import server.ReplicatedServer;
//
//import com.datastax.driver.core.Cluster;
//import com.datastax.driver.core.Session;
//import org.apache.zookeeper.*;
//import org.apache.zookeeper.data.Stat;
//import org.json.JSONException;
//import org.json.JSONObject;
//
//import java.io.IOException;
//import java.net.InetSocketAddress;
//import java.nio.charset.StandardCharsets;
//import java.util.*;
//import java.util.concurrent.CountDownLatch;
//import java.util.logging.Level;
//import java.util.logging.Logger;
//import java.util.Base64;
//
//public class MyDBFaultTolerantServerZK extends MyDBSingleServer implements Watcher {
//	private static final Logger log = Logger.getLogger(MyDBFaultTolerantServerZK.class.getName());
//
//	public static final int SLEEP = 1000;
//	public static final boolean DROP_TABLES_AFTER_TESTS = true;
//	public static final int MAX_LOG_SIZE = 400;
//
//	private static final String ZK_CONNECT_DEFAULT = "127.0.0.1:2181";
//	private static final int ZK_SESSION_TIMEOUT = 30000;
//	private static final String OPS_ROOT = "/operations";
//	private static final String CKPT_ROOT = "/checkpoints";
//	private static final String OP_PREFIX = "op-";
//
//	private final Cluster cluster;
//	private final Session session;
//	private final String myID;
//
//	private final ZooKeeper zk;
//	private final String zkConnectString;
//
//	private long lastApplied = -1L;
//
//	private final LinkedHashMap<Long, String> logCache = new LinkedHashMap<Long, String>() {
//		@Override
//		protected boolean removeEldestEntry(Map.Entry<Long, String> eldest) {
//			return size() > MAX_LOG_SIZE;
//		}
//	};
//
//	private final Object processLock = new Object();
//
//	public MyDBFaultTolerantServerZK(NodeConfig<String> nodeConfig, String myID, InetSocketAddress isaDB) throws IOException {
//		super(new InetSocketAddress(nodeConfig.getNodeAddress(myID),
//						nodeConfig.getNodePort(myID) - ReplicatedServer.SERVER_PORT_OFFSET),
//				isaDB, myID);
//
//		this.myID = myID;
//
//		// Cassandra init
//		String contactPoint = isaDB.getAddress().getHostAddress();
//		log.log(Level.INFO, "Connecting to Cassandra contact point {0}, keyspace {1}", new Object[]{contactPoint, myID});
//		this.cluster = Cluster.builder().addContactPoint(contactPoint).build();
//		this.session = cluster.connect(myID);
//
//		// ZooKeeper init
//		this.zkConnectString = ZK_CONNECT_DEFAULT;
//		try {
//			CountDownLatch connectedSignal = new CountDownLatch(1);
//			this.zk = new ZooKeeper(this.zkConnectString, ZK_SESSION_TIMEOUT, event -> {
//				if (event.getState() == Event.KeeperState.SyncConnected) {
//					connectedSignal.countDown();
//				}
//				process(event);
//			});
//			connectedSignal.await();
//
//			ensurePathExists(OPS_ROOT);
//			ensurePathExists(CKPT_ROOT);
//			ensurePathExists(CKPT_ROOT + "/" + myID);
//
//			recoverFromCheckpoint();
//
//			replayUnappliedOperations();
//
//			new Thread(() -> {
//				try {
//					Thread.sleep(2L * SLEEP);
//					try {
//						replayUnappliedOperations();
//					} catch (KeeperException | InterruptedException e) {
//						log.log(Level.WARNING, "Background replay failed: {0}", e.getMessage());
//					}
//				} catch (InterruptedException ie) {
//					Thread.currentThread().interrupt();
//				}
//			}, "ZK-Replay-SafetyNet-" + myID).start();
//
//			log.log(Level.INFO, "MyDBFaultTolerantServerZK {0} started: zk={1}", new Object[]{myID, zkConnectString});
//
//		} catch (InterruptedException ie) {
//			Thread.currentThread().interrupt();
//			throw new IOException("Interrupted waiting for ZooKeeper connection", ie);
//		} catch (KeeperException ke) {
//			throw new IOException("Failed to initialize ZooKeeper: " + ke.getMessage(), ke);
//		} catch (Exception e) {
//			throw new IOException("Failed to initialize ZooKeeper: " + e.getMessage(), e);
//		}
//	}
//
//	private void ensurePathExists(String path) throws KeeperException, InterruptedException {
//		String[] parts = path.split("/");
//		String cur = "";
//		for (String p : parts) {
//			if (p.length() == 0) continue;
//			cur = cur + "/" + p;
//			Stat s = zk.exists(cur, false);
//			if (s == null) {
//				try {
//					zk.create(cur, new byte[0], ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
//				} catch (KeeperException.NodeExistsException ignore) {}
//			}
//		}
//	}
//
//	@Override
//	protected void handleMessageFromClient(byte[] bytes, NIOHeader header) {
//		String reqStr = new String(bytes, StandardCharsets.UTF_8);
//		try {
//			List<String> children = zk.getChildren(OPS_ROOT, false);
//			if (children.size() >= MAX_LOG_SIZE) {
//				this.clientMessenger.send(header.sndr, "[ERROR: queue-full]".getBytes(StandardCharsets.UTF_8));
//				return;
//			}
//
//			byte[] data = reqStr.getBytes(StandardCharsets.UTF_8);
//			zk.create(OPS_ROOT + "/" + OP_PREFIX, data, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT_SEQUENTIAL);
//
//			this.clientMessenger.send(header.sndr, ("[success:" + reqStr + "]").getBytes(StandardCharsets.UTF_8));
//
//		} catch (KeeperException | InterruptedException | IOException e) {
//			try {
//				this.clientMessenger.send(header.sndr, "[ERROR:failed]".getBytes(StandardCharsets.UTF_8));
//			} catch (IOException ignored) {}
//			log.log(Level.SEVERE, "Error handling client request: {0}", e.getMessage());
//		}
//	}
//
//	protected void handleMessageFromServer(byte[] bytes, NIOHeader header) {}
//
//	@Override
//	public void process(WatchedEvent event) {
//		if (event == null) return;
//		try {
//			if (event.getType() == Event.EventType.NodeChildrenChanged && OPS_ROOT.equals(event.getPath())) {
//				replayUnappliedOperations();
//			}
//			if (event.getState() == Event.KeeperState.Expired) {
//				log.log(Level.WARNING, "ZooKeeper session expired, restart may be required.");
//			}
//		} catch (Exception e) {
//			log.log(Level.SEVERE, "Error in watcher process: {0}", e.getMessage());
//		}
//	}
//
//	private void replayUnappliedOperations() throws KeeperException, InterruptedException {
//		synchronized (processLock) {
//			List<String> children = zk.getChildren(OPS_ROOT, this);
//			if (children == null || children.isEmpty()) return;
//			Collections.sort(children);
//
//			for (String child : children) {
//				long seq = seqFromName(child);
//				if (seq <= lastApplied) continue;
//				String path = OPS_ROOT + "/" + child;
//
//				Stat s = zk.exists(path, false);
//				if (s == null) continue;
//				byte[] data = zk.getData(path, false, s);
//				if (data == null) continue;
//				String reqStr = new String(data, StandardCharsets.UTF_8);
//
//				boolean ok = applyRequestToCassandra(reqStr);
//				if (ok) {
//					lastApplied = seq;
//					synchronized (logCache) { logCache.put(seq, reqStr); }
//					if (logCache.size() >= MAX_LOG_SIZE) checkpoint();
//				} else {
//					break;
//				}
//			}
//		}
//	}
//
//	private long seqFromName(String child) {
//		String name = child.contains("/") ? child.substring(child.lastIndexOf('/') + 1) : child;
//		int dash = name.lastIndexOf('-');
//		if (dash < 0 || dash + 1 >= name.length()) return Long.MAX_VALUE;
//		try { return Long.parseLong(name.substring(dash + 1)); }
//		catch (NumberFormatException e) { return Long.MAX_VALUE; }
//	}
//
//	private boolean applyRequestToCassandra(String requestString) {
//		try {
//			String cql;
//			try {
//				JSONObject json = new JSONObject(requestString);
//				if (json.has("request")) cql = json.getString("request");
//				else if (json.has("REQUEST")) cql = json.getString("REQUEST");
//				else if (json.has("req")) cql = json.getString("req");
//				else if (json.has("cql")) cql = json.getString("cql");
//				else cql = requestString;
//			} catch (JSONException je) { cql = requestString; }
//
//			session.execute(cql);
//			return true;
//		} catch (Exception e) {
//			log.log(Level.SEVERE, "Cassandra application error: {0}", e.getMessage());
//			return false;
//		}
//	}
//
//	private void checkpoint() throws KeeperException, InterruptedException {
//		synchronized (processLock) {
//			String ckptPath = CKPT_ROOT + "/" + myID;
//			StringBuilder sb = new StringBuilder();
//			sb.append("lastApplied=").append(lastApplied).append('\n');
//			synchronized (logCache) {
//				for (Map.Entry<Long, String> e : logCache.entrySet()) {
//					long seq = e.getKey();
//					if (seq > lastApplied) continue;
//					sb.append(seq).append('|')
//							.append(Base64.getEncoder().encodeToString(e.getValue().getBytes(StandardCharsets.UTF_8)))
//							.append('\n');
//				}
//			}
//
//			byte[] snapshotBytes = sb.toString().getBytes(StandardCharsets.UTF_8);
//
//			Stat s = zk.exists(ckptPath, false);
//			if (s != null) zk.setData(ckptPath, snapshotBytes, -1);
//			else { ensurePathExists(ckptPath); zk.setData(ckptPath, snapshotBytes, -1); }
//
//			List<String> children = zk.getChildren(OPS_ROOT, false);
//			if (children != null) {
//				for (String child : children) {
//					long seq = seqFromName(child);
//					if (seq <= lastApplied) {
//						try { zk.delete(OPS_ROOT + "/" + child, -1); }
//						catch (KeeperException.NoNodeException | KeeperException.NotEmptyException ignore) {}
//					}
//				}
//			}
//		}
//	}
//
//	private void recoverFromCheckpoint() {
//		try {
//			String ckptPath = CKPT_ROOT + "/" + myID;
//			Stat s = zk.exists(ckptPath, false);
//			if (s == null) return;
//
//			byte[] data = zk.getData(ckptPath, false, s);
//			if (data == null || data.length == 0) return;
//
//			String sstr = new String(data, StandardCharsets.UTF_8);
//			String[] lines = sstr.split("\\r?\\n");
//
//			if (lines.length == 0) return;
//
//			long parsedLast = -1L;
//			String first = lines[0].trim();
//			if (first.startsWith("lastApplied=")) {
//				try { parsedLast = Long.parseLong(first.substring("lastApplied=".length()).trim()); }
//				catch (NumberFormatException nfe) { parsedLast = -1L; }
//			} else {
//				try { parsedLast = Long.parseLong(first.trim()); } catch (NumberFormatException nfe) { parsedLast = -1L; }
//			}
//			if (parsedLast >= 0) lastApplied = parsedLast;
//
//			if (lines.length > 1) {
//				TreeMap<Long, String> restored = new TreeMap<>();
//				for (int i = 1; i < lines.length; i++) {
//					String ln = lines[i].trim();
//					if (ln.isEmpty()) continue;
//					int sep = ln.indexOf('|');
//					if (sep <= 0) continue;
//					try {
//						long seq = Long.parseLong(ln.substring(0, sep));
//						byte[] payloadBytes = Base64.getDecoder().decode(ln.substring(sep + 1));
//						restored.put(seq, new String(payloadBytes, StandardCharsets.UTF_8));
//					} catch (Exception ignored) {}
//				}
//
//				if (!restored.isEmpty()) {
//					synchronized (logCache) { logCache.clear(); logCache.putAll(restored); }
//
//					// Restore ZK log entries for replay
//					for (Map.Entry<Long, String> e : restored.entrySet()) {
//						long seq = e.getKey();
//						String payload = e.getValue();
//						String zkPath = OPS_ROOT + "/" + OP_PREFIX + String.format("%010d", seq);
//						Stat exists = zk.exists(zkPath, false);
//						if (exists == null) {
//							zk.create(zkPath, payload.getBytes(StandardCharsets.UTF_8),
//									ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
//						}
//					}
//					// Trigger replay
//					replayUnappliedOperations();
//				}
//			}
//
//			log.log(Level.INFO, "Recovered checkpoint: lastApplied={0}", lastApplied);
//
//		} catch (Exception e) {
//			log.log(Level.WARNING, "Failed to recover checkpoint: {0}", e.getMessage());
//		}
//	}
//
//	@Override
//	public void close() {
//		super.close();
//		try { if (zk != null) zk.close(); } catch (InterruptedException e) { Thread.currentThread().interrupt(); }
//		try { if (session != null) session.close(); } catch (Exception ignored) {}
//		try { if (cluster != null) cluster.close(); } catch (Exception ignored) {}
//		log.log(Level.INFO, "MyDBFaultTolerantServerZK {0} closed", myID);
//	}
//
//	public static void main(String[] args) throws IOException {
//		NodeConfig<String> nc = NodeConfigUtils.getNodeConfigFromFile(args[0], ReplicatedServer.SERVER_PREFIX, ReplicatedServer.SERVER_PORT_OFFSET);
//		String myID = args[1];
//		InetSocketAddress isaDB = args.length > 2 ? Util.getInetSocketAddressFromString(args[2]) : new InetSocketAddress("localhost", 9042);
//		new MyDBFaultTolerantServerZK(nc, myID, isaDB);
//	}
//}

package server.faulttolerance;

import edu.umass.cs.nio.nioutils.NIOHeader;
import edu.umass.cs.nio.nioutils.NodeConfigUtils;
import edu.umass.cs.nio.interfaces.NodeConfig;
import edu.umass.cs.utils.Util;
import server.MyDBSingleServer;
import server.ReplicatedServer;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Session;
import org.apache.zookeeper.*;
import org.apache.zookeeper.data.Stat;
import org.json.JSONException;
import org.json.JSONObject;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.concurrent.*;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.Base64;

public class MyDBFaultTolerantServerZK extends MyDBSingleServer implements Watcher {
	private static final Logger log = Logger.getLogger(MyDBFaultTolerantServerZK.class.getName());

	public static final int SLEEP = 1000;
	public static final boolean DROP_TABLES_AFTER_TESTS = true;
	public static final int MAX_LOG_SIZE = 400;

	private static final String ZK_CONNECT_DEFAULT = "127.0.0.1:2181";
	private static final int ZK_SESSION_TIMEOUT = 30000;
	private static final String OPS_ROOT = "/operations";
	private static final String CKPT_ROOT = "/checkpoints";
	private static final String OP_PREFIX = "op-";

	private final Cluster cluster;
	private final Session session;
	private final String myID;

	private final ZooKeeper zk;
	private final String zkConnectString;

	private long lastApplied = -1L;
	private int operationsSinceLastCheckpoint = 0;
	private static final int CHECKPOINT_INTERVAL = 50;

	private final LinkedHashMap<Long, String> logCache = new LinkedHashMap<Long, String>() {
		@Override
		protected boolean removeEldestEntry(Map.Entry<Long, String> eldest) {
			return size() > MAX_LOG_SIZE;
		}
	};

	private final Object processLock = new Object();
	private volatile boolean recovering = false;

	private final Map<Long, CountDownLatch> completionLatches = new ConcurrentHashMap<>();

	private final ScheduledExecutorService checkpointExecutor = Executors.newSingleThreadScheduledExecutor();

	public MyDBFaultTolerantServerZK(NodeConfig<String> nodeConfig, String myID, InetSocketAddress isaDB) throws IOException {
		super(new InetSocketAddress(nodeConfig.getNodeAddress(myID),
						nodeConfig.getNodePort(myID) - ReplicatedServer.SERVER_PORT_OFFSET),
				isaDB, myID);

		this.myID = myID;

		// Cassandra init
		String contactPoint = isaDB.getAddress().getHostAddress();
		System.out.println("[INIT] " + myID + " connecting to Cassandra at " + contactPoint);
		this.cluster = Cluster.builder().addContactPoint(contactPoint).build();
		this.session = cluster.connect(myID);

		// ZooKeeper init
		this.zkConnectString = ZK_CONNECT_DEFAULT;
		try {
			CountDownLatch connectedSignal = new CountDownLatch(1);
			this.zk = new ZooKeeper(this.zkConnectString, ZK_SESSION_TIMEOUT, event -> {
				if (event.getState() == Event.KeeperState.SyncConnected) {
					connectedSignal.countDown();
				}
				process(event);
			});
			connectedSignal.await();
			System.out.println("[INIT] " + myID + " connected to ZooKeeper");

			ensurePathExists(OPS_ROOT);
			ensurePathExists(CKPT_ROOT);

			// Recovery phase
			recovering = true;
			recoverFromCheckpoint();

			// Check what's in ZooKeeper
			List<String> opsInZk = zk.getChildren(OPS_ROOT, false);
			System.out.println("[INIT] " + myID + " found " + opsInZk.size() + " operations in ZooKeeper");

			replayUnappliedOperations();
			recovering = false;

			// Periodic checkpointing
			checkpointExecutor.scheduleAtFixedRate(() -> {
				try {
					if (operationsSinceLastCheckpoint > 0) {
						synchronized (processLock) {
							checkpoint();
						}
					}
				} catch (Exception e) {
					System.err.println("[CHECKPOINT] " + myID + " periodic checkpoint failed: " + e.getMessage());
				}
			}, 2, 2, TimeUnit.SECONDS);

			System.out.println("[INIT] " + myID + " ready, lastApplied=" + lastApplied);

		} catch (InterruptedException ie) {
			Thread.currentThread().interrupt();
			throw new IOException("Interrupted waiting for ZooKeeper connection", ie);
		} catch (KeeperException ke) {
			throw new IOException("Failed to initialize ZooKeeper: " + ke.getMessage(), ke);
		} catch (Exception e) {
			throw new IOException("Failed to initialize ZooKeeper: " + e.getMessage(), e);
		}
	}

	private void ensurePathExists(String path) throws KeeperException, InterruptedException {
		if (path == null || path.isEmpty()) return;
		String[] parts = path.split("/");
		String cur = "";
		for (String p : parts) {
			if (p.length() == 0) continue;
			cur = cur + "/" + p;
			Stat s = zk.exists(cur, false);
			if (s == null) {
				try {
					zk.create(cur, new byte[0], ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
					System.out.println("[ZK] " + myID + " created path: " + cur);
				} catch (KeeperException.NodeExistsException ignore) {}
			}
		}
	}

	@Override
	protected void handleMessageFromClient(byte[] bytes, NIOHeader header) {
		String reqStr = new String(bytes, StandardCharsets.UTF_8);
		try {
			// Write to ZooKeeper
			byte[] data = reqStr.getBytes(StandardCharsets.UTF_8);
			String createdPath = zk.create(OPS_ROOT + "/" + OP_PREFIX, data,
					ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT_SEQUENTIAL);

			long seq = seqFromName(createdPath);
			System.out.println("[CLIENT] " + myID + " created operation: " + createdPath + " (seq=" + seq + ")");

			// Wait for application
			CountDownLatch latch = new CountDownLatch(1);
			completionLatches.put(seq, latch);

			replayUnappliedOperations();

			boolean applied = latch.await(10, TimeUnit.SECONDS);
			completionLatches.remove(seq);

			if (applied) {
				System.out.println("[CLIENT] " + myID + " completed operation seq=" + seq);
				this.clientMessenger.send(header.sndr, ("[success:" + reqStr + "]").getBytes(StandardCharsets.UTF_8));
			} else {
				System.err.println("[CLIENT] " + myID + " TIMEOUT on operation seq=" + seq);
				this.clientMessenger.send(header.sndr, "[ERROR:timeout]".getBytes(StandardCharsets.UTF_8));
			}

		} catch (Exception e) {
			try {
				this.clientMessenger.send(header.sndr, "[ERROR:failed]".getBytes(StandardCharsets.UTF_8));
			} catch (IOException ignored) {}
			System.err.println("[CLIENT] " + myID + " error: " + e.getMessage());
			e.printStackTrace();
		}
	}

	protected void handleMessageFromServer(byte[] bytes, NIOHeader header) {}

	@Override
	public void process(WatchedEvent event) {
		if (event == null) return;
		try {
			if (event.getType() == Event.EventType.NodeChildrenChanged && OPS_ROOT.equals(event.getPath())) {
				System.out.println("[WATCH] " + myID + " detected children changed");
				if (!recovering) {
					replayUnappliedOperations();
				}
			}
		} catch (Exception e) {
			System.err.println("[WATCH] " + myID + " error: " + e.getMessage());
		}
	}

	private void replayUnappliedOperations() throws KeeperException, InterruptedException {
		synchronized (processLock) {
			List<String> children = zk.getChildren(OPS_ROOT, true);
			if (children == null || children.isEmpty()) {
				System.out.println("[REPLAY] " + myID + " no operations to replay");
				return;
			}

			Collections.sort(children);
			System.out.println("[REPLAY] " + myID + " found " + children.size() + " operations, lastApplied=" + lastApplied);

			int appliedCount = 0;
			for (String child : children) {
				long seq = seqFromName(child);
				if (seq <= lastApplied) {
					continue;
				}

				String path = OPS_ROOT + "/" + child;
				try {
					Stat s = zk.exists(path, false);
					if (s == null) {
						System.out.println("[REPLAY] " + myID + " path disappeared: " + path);
						continue;
					}

					byte[] data = zk.getData(path, false, s);
					if (data == null || data.length == 0) {
						System.err.println("[REPLAY] " + myID + " empty data at: " + path);
						continue;
					}

					String reqStr = new String(data, StandardCharsets.UTF_8);

					boolean success = applyRequestToCassandra(reqStr);
					if (success) {
						lastApplied = seq;
						synchronized (logCache) {
							logCache.put(seq, reqStr);
						}
						operationsSinceLastCheckpoint++;
						appliedCount++;

						// Signal completion
						CountDownLatch latch = completionLatches.get(seq);
						if (latch != null) {
							latch.countDown();
						}

						// Checkpoint periodically
						if (operationsSinceLastCheckpoint >= CHECKPOINT_INTERVAL) {
							checkpoint();
						}
					} else {
						System.err.println("[REPLAY] " + myID + " failed to apply seq=" + seq);
					}
				} catch (KeeperException.NoNodeException e) {
					System.err.println("[REPLAY] " + myID + " node deleted: " + path);
				}
			}

			if (appliedCount > 0) {
				System.out.println("[REPLAY] " + myID + " applied " + appliedCount + " operations, lastApplied now=" + lastApplied);
			}
		}
	}

	private long seqFromName(String child) {
		String name = child.contains("/") ? child.substring(child.lastIndexOf('/') + 1) : child;
		int dash = name.lastIndexOf('-');
		if (dash < 0 || dash + 1 >= name.length()) {
			return Long.MAX_VALUE;
		}
		try {
			return Long.parseLong(name.substring(dash + 1));
		} catch (NumberFormatException e) {
			return Long.MAX_VALUE;
		}
	}

	private boolean applyRequestToCassandra(String requestString) {
		try {
			String cql;
			try {
				JSONObject json = new JSONObject(requestString);
				if (json.has("request")) cql = json.getString("request");
				else if (json.has("REQUEST")) cql = json.getString("REQUEST");
				else if (json.has("req")) cql = json.getString("req");
				else if (json.has("cql")) cql = json.getString("cql");
				else cql = requestString;
			} catch (JSONException je) {
				cql = requestString;
			}

			session.execute(cql);
			return true;
		} catch (Exception e) {
			System.err.println("[CASSANDRA] " + myID + " error: " + e.getMessage());
			return false;
		}
	}

	private void checkpoint() throws KeeperException, InterruptedException {
		String ckptPath = CKPT_ROOT + "/" + myID;

		ensurePathExists(ckptPath);

		StringBuilder sb = new StringBuilder();
		sb.append("lastApplied=").append(lastApplied).append('\n');

		synchronized (logCache) {
			for (Map.Entry<Long, String> e : logCache.entrySet()) {
				long seq = e.getKey();
				if (seq > lastApplied) continue;
				sb.append(seq).append('|')
						.append(Base64.getEncoder().encodeToString(e.getValue().getBytes(StandardCharsets.UTF_8)))
						.append('\n');
			}
		}

		byte[] snapshotBytes = sb.toString().getBytes(StandardCharsets.UTF_8);

		Stat s = zk.exists(ckptPath, false);
		if (s != null) {
			zk.setData(ckptPath, snapshotBytes, -1);
		} else {
			zk.create(ckptPath, snapshotBytes, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
		}

		operationsSinceLastCheckpoint = 0;
		System.out.println("[CHECKPOINT] " + myID + " saved checkpoint, lastApplied=" + lastApplied + ", cache size=" + logCache.size());

		// Delete old operations
		long safeDeleteBefore = lastApplied - (MAX_LOG_SIZE * 3);
		if (safeDeleteBefore > 0) {
			List<String> children = zk.getChildren(OPS_ROOT, false);
			int deleted = 0;
			if (children != null) {
				for (String child : children) {
					long seq = seqFromName(child);
					if (seq < safeDeleteBefore) {
						try {
							zk.delete(OPS_ROOT + "/" + child, -1);
							deleted++;
						} catch (Exception ignore) {}
					}
				}
			}
			if (deleted > 0) {
				System.out.println("[CHECKPOINT] " + myID + " deleted " + deleted + " old operations");
			}
		}
	}

	private void recoverFromCheckpoint() {
		try {
			String ckptPath = CKPT_ROOT + "/" + myID;
			Stat s = zk.exists(ckptPath, false);
			if (s == null) {
				System.out.println("[RECOVER] " + myID + " no checkpoint found, will replay all from ZooKeeper");
				// No checkpoint - will start from beginning
				lastApplied = -1L;
				return;
			}

			byte[] data = zk.getData(ckptPath, false, s);
			if (data == null || data.length == 0) {
				System.out.println("[RECOVER] " + myID + " empty checkpoint, will replay all from ZooKeeper");
				lastApplied = -1L;
				return;
			}

			String sstr = new String(data, StandardCharsets.UTF_8);
			String[] lines = sstr.split("\\r?\\n");
			if (lines.length == 0) {
				lastApplied = -1L;
				return;
			}

			// Parse lastApplied from checkpoint
			long checkpointLastApplied = -1L;
			String first = lines[0].trim();
			if (first.startsWith("lastApplied=")) {
				try {
					checkpointLastApplied = Long.parseLong(first.substring("lastApplied=".length()).trim());
					System.out.println("[RECOVER] " + myID + " checkpoint has lastApplied=" + checkpointLastApplied);
				} catch (NumberFormatException nfe) {
					checkpointLastApplied = -1L;
				}
			}

			// Restore and RE-APPLY operations from checkpoint to rebuild Cassandra state
			if (lines.length > 1) {
				TreeMap<Long, String> restored = new TreeMap<>();
				for (int i = 1; i < lines.length; i++) {
					String ln = lines[i].trim();
					if (ln.isEmpty()) continue;
					int sep = ln.indexOf('|');
					if (sep <= 0) continue;
					try {
						long seq = Long.parseLong(ln.substring(0, sep));
						byte[] payloadBytes = Base64.getDecoder().decode(ln.substring(sep + 1));
						restored.put(seq, new String(payloadBytes, StandardCharsets.UTF_8));
					} catch (Exception ignored) {}
				}

				if (!restored.isEmpty()) {
					System.out.println("[RECOVER] " + myID + " re-applying " + restored.size() + " operations from checkpoint to Cassandra");

					// Re-apply all checkpointed operations to Cassandra
					for (Map.Entry<Long, String> e : restored.entrySet()) {
						long seq = e.getKey();
						String reqStr = e.getValue();

						boolean success = applyRequestToCassandra(reqStr);
						if (success) {
							lastApplied = seq;
						} else {
							System.err.println("[RECOVER] " + myID + " FAILED to re-apply seq=" + seq);
						}
					}

					// Restore log cache
					synchronized (logCache) {
						logCache.clear();
						logCache.putAll(restored);
					}

					System.out.println("[RECOVER] " + myID + " recovery from checkpoint complete, lastApplied=" + lastApplied);
				}
			} else if (checkpointLastApplied >= 0) {
				lastApplied = checkpointLastApplied;
			}

			// CRITICAL: After recovering from checkpoint, we MUST replay ALL operations
			// from ZooKeeper that come AFTER our checkpoint, because those operations
			// were committed to ZooKeeper but not yet checkpointed when we crashed
			System.out.println("[RECOVER] " + myID + " will now replay unapplied operations from ZooKeeper (seq > " + lastApplied + ")");

		} catch (Exception e) {
			System.err.println("[RECOVER] " + myID + " failed: " + e.getMessage());
			e.printStackTrace();
			lastApplied = -1L; // Start from beginning if recovery fails
		}
	}

	@Override
	public void close() {
		System.out.println("[CLOSE] " + myID + " shutting down, lastApplied=" + lastApplied);
		super.close();
		checkpointExecutor.shutdown();
		try {
			synchronized (processLock) {
				if (lastApplied >= 0) {
					checkpoint();
				}
			}
		} catch (Exception e) {
			System.err.println("[CLOSE] " + myID + " final checkpoint failed: " + e.getMessage());
		}
		try { if (zk != null) zk.close(); } catch (InterruptedException e) { Thread.currentThread().interrupt(); }
		try { if (session != null) session.close(); } catch (Exception ignored) {}
		try { if (cluster != null) cluster.close(); } catch (Exception ignored) {}
		System.out.println("[CLOSE] " + myID + " closed");
	}

	public static void main(String[] args) throws IOException {
		NodeConfig<String> nc = NodeConfigUtils.getNodeConfigFromFile(args[0], ReplicatedServer.SERVER_PREFIX, ReplicatedServer.SERVER_PORT_OFFSET);
		String myID = args[1];
		InetSocketAddress isaDB = args.length > 2 ? Util.getInetSocketAddressFromString(args[2]) : new InetSocketAddress("localhost", 9042);
		new MyDBFaultTolerantServerZK(nc, myID, isaDB);
	}
}