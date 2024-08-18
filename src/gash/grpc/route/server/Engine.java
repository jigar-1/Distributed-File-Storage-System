package gash.grpc.route.server;

import java.util.*;
import java.util.Properties;
import java.util.concurrent.LinkedBlockingDeque;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Core components to process work; shared with/across all sessions.
 * 
 * @author gash
 *
 */
public class Engine {

	protected static Logger logger = LoggerFactory.getLogger("server");
	private static Engine instance;
	private static Properties conf;

	protected String serverName;
	protected Long serverID;
	protected String serverRole;
	protected Integer serverPort;
	private Long nextMessageID;
	protected Long leaderServerID;
	

	/* workQueue/containers */
	protected LinkedBlockingDeque<Work> workQueue, mgmtQueue, queryQueue;

	/* worker threads */
	protected ArrayList<Worker> workers;
	protected ArrayList<QueryWorker> queryWorkers;

	protected MgmtWorker manager;

	/* connectivity */
	protected ArrayList<Link> links;

	protected HashMap<String, String> map;
	
	public HashMap<String, String> getHashMap(){
		return map;
	}
	
	public static Properties getConf() {
		return conf;
	}

	public static void configure(Properties conf) {
		if (Engine.conf == null) {
			Engine.conf = conf;
			instance = new Engine();
			instance.init();
		}
	}

	public static Engine getInstance() {
		if (instance == null)
			throw new RuntimeException("Engine not initialized");

		return instance;
	}

	private Engine() {
	}

	private synchronized void init() {
		if (conf == null) {
			Engine.logger.error("server is not configured!");
			throw new RuntimeException("server not configured!");
		}

		if (manager != null) {
			Engine.logger.error("trying to re-init() logistics!");
			return;
		}

		// extract settings. Here we are using basic properties which, requires
		// type checking and should also include range checking as well.

//		map = MapUtil.read(conf.getProperty("keysLocation"));
		map = new HashMap<>();
		String tmp = conf.getProperty("server.id");
		if (tmp == null)
			throw new RuntimeException("missing server ID");
		serverID = Long.parseLong(tmp);

		tmp = conf.getProperty("server.port");
		if (tmp == null)
			throw new RuntimeException("missing server port");
		serverPort = Integer.parseInt(tmp);
		if (serverPort <= 1024)
			throw new RuntimeException("server port must be above 1024");
		
		tmp = conf.getProperty("server.role");
		if (tmp == null)
			throw new RuntimeException("missing server role");
		serverRole = tmp;
		
		tmp = conf.getProperty("leader.server.id");
		if(tmp == null)
			throw new RuntimeException("missing server id");
		leaderServerID = Long.parseLong(tmp);
		
		// monotonically increasing number
		nextMessageID = 0L;

		// our list of connections to other servers
		links = new ArrayList<Link>();

		Engine.logger.info("starting queues");
		workQueue = new LinkedBlockingDeque<Work>();
		mgmtQueue = new LinkedBlockingDeque<Work>();
		queryQueue = new LinkedBlockingDeque<Work>();

		Engine.logger.info("starting workers");
		workers = new ArrayList<Worker>();
		var w = new Worker();
		workers.add(w);
		w.start();
		
		Engine.logger.info("starting query workers");
		queryWorkers = new ArrayList<QueryWorker>();
		var qw = new QueryWorker();
		queryWorkers.add(qw);
		qw.start();

		Engine.logger.info("starting manager");
		manager = new MgmtWorker();
		manager.start();

		Engine.logger.info("initializaton complete");
	}

	public synchronized void shutdown(boolean hard) {
		Engine.logger.info("server shutting down.");

		if (!hard && workQueue.size() > 0) {
			try {
				while (workQueue.size() > 0) {
					Thread.sleep(2000);
				}
			} catch (InterruptedException e) {
				Engine.logger.error("Waiting for work queue to empty interrupted, shutdown hard", e);
			}
		}
		for (var w : workers) {
			w.shutdown();
		}

		manager.shutdown();
	}

	public synchronized void increaseWorkers() {
		var w = new Worker();
		workers.add(0, w);
		w.start();
	}

	public synchronized void decreaseWorkers() {
		if (workers.size() > 0) {
			var w = workers.remove(0);
			w.shutdown();
		}
	}

	public Long getServerID() {
		return serverID;
	}

	public synchronized Long getNextMessageID() {
		// TODO this should be a hash value (but we want to retain the implicit
		// ordering effect of an increasing number)
		if (nextMessageID == Long.MAX_VALUE)
			nextMessageID = 0l;

		return ++nextMessageID;
	}

	public Integer getServerPort() {
		return serverPort;
	}
	
	public String getServerRole() {
		return serverRole;
	}
	
	public void setLeaderId(long id) {
		leaderServerID = id;
	}
}
