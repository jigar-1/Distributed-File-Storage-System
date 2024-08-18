package gash.grpc.route.server;

import java.io.BufferedReader;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.util.ArrayList;
import java.util.Properties;

import com.google.protobuf.ByteString;

import io.grpc.Server;
import io.grpc.ServerBuilder;
import io.grpc.stub.StreamObserver;
import route.RouteServiceGrpc.RouteServiceImplBase;

/**
 * copyright 2021, gash
 *
 * Gash licenses this file to you under the Apache License, version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at:
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
public class RouteServerImpl extends RouteServiceImplBase {
	private Server svr;
	private static int numberOfServers = 4;
	private static int responseCounter = 0;
	private static byte[] responseByte = new byte[0];
	private static int workQueueThreshold = 10, queryQueueThreshold = 10;

	/**
	 * Configuration of the server's identity, port, and role
	 */
	private static Properties getConfiguration(final File path) throws IOException {
		if (!path.exists())
			throw new IOException("missing file");

		Properties rtn = new Properties();
		FileInputStream fis = null;
		try {
			fis = new FileInputStream(path);
			rtn.load(fis);
		} finally {
			if (fis != null) {
				try {
					fis.close();
				} catch (IOException e) {
					// ignore
				}
			}
		}

		return rtn;
	}

	protected ByteString ack(route.Route msg) {
		// TODO complete processing
		final String blank = msg.getId()+ "accepted";
		byte[] raw = blank.getBytes();

		return ByteString.copyFrom(raw);
	}

	public static void main(String[] args) throws Exception {
		// TODO check args!

		String path = args[0];
		try {
			Properties conf = RouteServerImpl.getConfiguration(new File(path));
			Engine.configure(conf);
			Engine.getConf();

			/* Similar to the socket, waiting for a connection */
			final RouteServerImpl impl = new RouteServerImpl();
			impl.start();
			impl.blockUntilShutdown();

		} catch (IOException e) {
			// TODO better error message
			e.printStackTrace();
		}
		finally {
			MapUtil.write(Engine.getInstance().map);
		}
	}

	private void start() throws Exception {
		svr = ServerBuilder.forPort(Engine.getInstance().getServerPort()).addService(new RouteServerImpl()).build();

		Engine.logger.info("starting server");
		svr.start();

		Runtime.getRuntime().addShutdownHook(new Thread() {
			@Override
			public void run() {
				RouteServerImpl.this.stop();
			}
		});
	}

	protected void stop() {
		svr.shutdown();
	}

	private void blockUntilShutdown() throws Exception {
		/* TODO what clean up is required? */
		svr.awaitTermination();

	}

	private boolean verify(route.Route request) {
		return true;
	}
	
	private route.Route.Builder	buildAck(String message, route.Route request){
		route.Route.Builder ack = route.Route.newBuilder();;
		byte[] raw = message.getBytes();

		// routing/header information
		ack.setId(Engine.getInstance().getNextMessageID());
		ack.setOrigin(Engine.getInstance().getServerID());
		ack.setDestination(request.getOrigin());
		ack.setPath(request.getPath());
		ack.setPayload(ByteString.copyFrom(raw));
		
		return ack;
	}
	
	
	private void forwardUploadRequest(route.Route request, boolean isLoadBalancedForward) {
		String linkIp = (String) Engine.getConf().getProperty("server.connects.ip");
		int linkPort = Integer.parseInt(Engine.getConf().getProperty("server.connects.port"));

		long dest = request.getDestination();
		dest = (dest + (isLoadBalancedForward ? 1 : 0)) % numberOfServers ;
		
		Sender sender = new Sender(linkIp, linkPort);				
		sender.sendRequest(request.getOrigin(), dest, request.getId(), request.getPath(), request.getPayload());
		
		Engine.logger.info("Upload Request forwarded to: " + request.getDestination());
		Engine.logger.info("TODO: send message");
	}
	
	private void forwardLeaderElectionRequest(route.Route request, long destid, String path) {
		String linkIp = (String) Engine.getConf().getProperty("server.connects.ip");
		int linkPort = Integer.parseInt(Engine.getConf().getProperty("server.connects.port"));
		
		Sender sender = new Sender(linkIp, linkPort);				
		sender.sendRequest(request.getOrigin(), destid, request.getId(), path, request.getPayload());
		
		Engine.logger.info("Leader Election Request forwarded to: " + request.getDestination());
		Engine.logger.info("TODO: send message");
	}
	
	private void forwardQuery(route.Route request, boolean isLoadBalancedForward) {
		String linkIp = (String) Engine.getConf().getProperty("server.connects.ip");
		int linkPort = Integer.parseInt(Engine.getConf().getProperty("server.connects.port"));

		long dest = request.getDestination();
		dest = (dest + (isLoadBalancedForward ? 1 : 0)) % numberOfServers ;
		
		Sender sender = new Sender(linkIp, linkPort);				
		sender.sendQuery(request.getId(),request.getOrigin(), dest, request.getPath(), request.getPayload());
		
		Engine.logger.info("Upload Request forwarded to: " + request.getDestination());
		Engine.logger.info("TODO: send message");
	}
	
	private void handleLeaderElection(route.Route request, StreamObserver<route.Route> responseObserver) {		
		if(request.getPath().compareToIgnoreCase("/initiateLeaderElection")==0) {
			this.forwardLeaderElectionRequest(request, Engine.getInstance().serverID, "/leaderElection");
		}
		if((request.getPath().compareToIgnoreCase("/leaderElection")==0)) {
			if(request.getDestination() == Engine.getInstance().serverID) {
				Engine.getInstance().setLeaderId( Engine.getInstance().serverID);
				this.forwardLeaderElectionRequest(request, Engine.getInstance().serverID, "/leaderElected");
			}
			else {
				if(Engine.getInstance().serverID < request.getDestination()) {
					this.forwardLeaderElectionRequest(request, request.getDestination(), request.getPath());
				}
				else {
					this.forwardLeaderElectionRequest(request, Engine.getInstance().serverID, request.getPath());
				}
			}
		}
		if((request.getPath().compareToIgnoreCase("/leaderElected")==0)) {
			if(request.getDestination()!=Engine.getInstance().serverID) {
				Engine.getInstance().setLeaderId(request.getDestination());
				this.forwardLeaderElectionRequest(request, request.getDestination(), request.getPath());
			}
			else {
				System.out.println("Election Complete! I am the leader!");
			}
		}
	}
	
	private void handleUpload(route.Route request, StreamObserver<route.Route> responseObserver) {
		if(request.getDestination() == Engine.getInstance().serverID) {
			if(Engine.getInstance().workQueue.size() <= workQueueThreshold) {
				Engine.getInstance().workQueue.add(new Work(request, responseObserver));
			}
			else {
				this.forwardUploadRequest(request, true);
			}
		}else {
			this.forwardUploadRequest(request, false);
		}
	}
	
	private void handleQuery(route.Route request, StreamObserver<route.Route> responseObserver) {
		String linkIp = (String) Engine.getConf().getProperty("server.connects.ip");
		int linkPort = Integer.parseInt(Engine.getConf().getProperty("server.connects.port"));

		Sender sender = new Sender(linkIp, linkPort);
		sender.sendQuery(request.getId(),request.getOrigin(), request.getDestination(), "/queryRequest", request.getPayload());
		
		Engine.logger.info("Upload Request forwarded to: " + request.getDestination());
		Engine.logger.info("TODO: send message");
		
	}
	
	private void handleQueryRequest(route.Route request, StreamObserver<route.Route> responseObserver) {
		if(Engine.getInstance().serverRole.compareToIgnoreCase("leader")==0) {
			Engine.logger.info("Query with Id : - "+request.getId()+" completed the ring !!! ");
		}else {
			if(Engine.getInstance().queryQueue.size() <= workQueueThreshold) {
				Engine.getInstance().queryQueue.add(new Work(request, responseObserver));
			}
			else {
				this.forwardQuery(request, true);
			}
		}
		
		
	}
	
	private void handleQueryResponse(route.Route response, StreamObserver<route.Route> responseObserver) {
		
		if(Engine.getInstance().serverRole.compareToIgnoreCase("leader")==0) {
			Engine.logger.info("QueryResponse with Id : - "+response.getId()+" compeleted the ring !!! ");
			if(response.getPath().compareToIgnoreCase("/queryResponse")==0)
			{
				if(RouteServerImpl.responseCounter++ < (numberOfServers - 1)) {
					byte[] payload = response.getPayload().toByteArray();
					try {
						Util.byteArrayToCSV(payload, "../../resources/CSVParse/csvSamples/queryOut.csv");
						
					} catch (Exception e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
					
					
					
				}
				System.out.println("Response counter increment" + RouteServerImpl.responseCounter);
//				Util.byteArrayToCSV(payload, filePath);	
			}
		}else{
			this.forwardQuery(response, false);
		}
			
	}
	
	private void handleRest(route.Route request, StreamObserver<route.Route> responseObserver) {
			
	}
	
	
	/**
	 * server received a message!
	 */
	@Override
	public void request(route.Route request, StreamObserver<route.Route> responseObserver) {

		// TODO refactor to use RouteServer to isolate implementation from
		// transportation

		// ack work
		route.Route.Builder ack =null;
		if (verify(request)) {

			// delay work
			var w = new Work(request, responseObserver);
			String path = w.request.getPath();
			
			if(path.compareToIgnoreCase("/initiateLeaderElection")==0) {	
				this.handleLeaderElection(request, responseObserver);
				ack = buildAck( "Leader Election initiated!", request);
			}
			
			if(path.compareToIgnoreCase("/leaderElection")==0) {	
				this.handleLeaderElection(request, responseObserver);
				ack = buildAck( "Leader Election initiated!", request);
			}
			
			if(path.compareToIgnoreCase("/leaderElected")==0) {	
				this.handleLeaderElection(request, responseObserver);
				ack = buildAck( "Leader Election handled!", request);
			}
			
			if(path.compareToIgnoreCase("/upload")==0) {
				
				if(w.request.getDestination()==5) {
					String linkIp = "192.168.0.221";
					int linkPort = 3038;

					Sender sender = new Sender(linkIp, linkPort);				
					sender.sendRequest(request.getOrigin(), request.getDestination(), request.getId(), request.getPath(), request.getPayload());
					
					Engine.logger.info("Upload Request forwarded to: " + request.getDestination());
					Engine.logger.info("TODO: send message");
				}else {
					this.handleUpload(request, responseObserver);
				}
				
				
				
				ack = buildAck( "Chunk upload handled!", request);
				
			}
			else {
				
				this.handleRest(request, responseObserver);
				ack = buildAck( "Path did not mathed !!!", request);
				
			}

			if (Engine.logger.isDebugEnabled())
				Engine.logger.debug("request() qsize = " + Engine.getInstance().workQueue.size());
			
		} 

		route.Route rtn = ack.build();
		responseObserver.onNext(rtn);
		responseObserver.onCompleted();
	}
	
	@Override
	public void queryInternal(route.Route request, StreamObserver<route.Route> responseObserver) {
		route.Route.Builder ack =null;
		String path = request.getPath();
		if(path.compareToIgnoreCase("/queryRequest")==0) {
			
			this.handleQueryRequest(request, responseObserver);
			ack = buildAck( "queryInternal forwarded !!!", request);
			
		}else if(path.compareToIgnoreCase("/queryResponse")==0){
			
			this.handleQueryResponse(request, responseObserver);
			ack = buildAck( "queryResponse Handled !!!", request);
			
			
		}else if(path.compareToIgnoreCase("/query")==0) {
			
			this.handleQuery(request, responseObserver);
			ack = buildAck( "Query handled", request);
			
		}else {
			
			this.handleRest(request, responseObserver);
			ack = buildAck( "Path did not mathed !!!", request);
			
		}
		
		route.Route rtn = ack.build();
		responseObserver.onNext(rtn);
		responseObserver.onCompleted();
	}
	
	
	@Override
	public void query(route.Route request, StreamObserver<route.Route> responseObserver) {
		
		this.handleQuery(request, responseObserver);
		route.Route.Builder ack = buildAck( "query injected in ring !!!", request);
		
		while(true) {
			if(RouteServerImpl.responseCounter == numberOfServers-1) {
				
				
				
				try {
					ack = route.Route.newBuilder();
					// routing/header information
					ack.setId(request.getId());
					ack.setOrigin(Engine.getInstance().getServerID());
					ack.setDestination(request.getOrigin());
					ack.setPath(request.getPath());
					
					File file = new File("../../../grpc-test-1/resources/CSVParse/csvSamples/queryOut.csv");
					ArrayList<String> arrayList = new ArrayList<>();
					FileReader fr; fr = new FileReader(file);
					BufferedReader br = new BufferedReader(fr);
					
					String line = "";
					while((line = br.readLine()) != null) {
						arrayList.add(line);
					}
					
					ByteArrayOutputStream baos = new ByteArrayOutputStream();
                	ObjectOutputStream oos = new ObjectOutputStream(baos);
                	oos.writeObject(arrayList);
                	ack.setPayload(ByteString.copyFrom(baos.toByteArray()));
                	Engine.logger.info("Byte Array size : " + baos.toByteArray().length);
    				responseObserver.onNext(ack.build());
    				
    				break;
					
				} catch (Exception e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
		        
				
				
				
			}
		}
		
		RouteServerImpl.responseCounter = 0;
		responseByte = new byte[0];
		Engine.logger.info("Final query send by Leader !!");
        responseObserver.onCompleted();
        
	}
}
