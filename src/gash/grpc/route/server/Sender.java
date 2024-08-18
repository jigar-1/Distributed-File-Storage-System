package gash.grpc.route.server;

import com.google.protobuf.ByteString;

import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import route.Route;
import route.RouteServiceGrpc;

public class Sender {
    
    // private int originId;
    private String ip;
    private int destPort;
    private long destQueryId;
    
    public Sender( String ip, int destPort)
    {
        // this.originId = originId;
        this.ip = ip;
        this.destPort = destPort;
    }
    
    public Sender(String ip, int destPort, long destQueryId)
    {
    	this.ip = ip;
    	this.destPort = destPort;
    	this.destQueryId = destQueryId;
    }

	private Route constructMessage(long mID, long toID,String path, ByteString payload, long originId) {
		Route.Builder bld = Route.newBuilder();
		bld.setId(mID);
		bld.setDestination(toID);
		bld.setOrigin(originId);
		bld.setPath(path);
		bld.setPayload(payload);

		return bld.build();
	}

	private void response(Route reply) {
		// TODO handle the reply/response from the server
		var payload = new String(reply.getPayload().toByteArray());
		System.out.println("reply: " + reply.getId() + ", from: " + reply.getOrigin() + ", payload: " + payload);
	}

	public void sendRequest(long originId, long destId, long msgId, String path, ByteString payload)
	{
		ManagedChannel ch = ManagedChannelBuilder.forAddress(ip, destPort).usePlaintext().build();
		RouteServiceGrpc.RouteServiceBlockingStub stub = RouteServiceGrpc.newBlockingStub(ch);
		
		var msg = this.constructMessage(msgId, destId, path, payload, originId);
		var r = stub.request(msg);
		response(r);
        ch.shutdown();
		
	}
	
	public void sendQuery(long msgId, long originId, long destId, String path, ByteString payload)
	{
		ManagedChannel ch = ManagedChannelBuilder.forAddress(ip, destPort).usePlaintext().build();
		RouteServiceGrpc.RouteServiceBlockingStub stub = RouteServiceGrpc.newBlockingStub(ch);
		
		var msg = this.constructMessage(msgId, destId, path, payload, originId);
		var r = stub.queryInternal(msg);
		response(r);
        ch.shutdown();
		
	}
	
	
	public void responseQuery(long msgId, long originId, long destId, String path, ByteString payload)
	{
		ManagedChannel ch = ManagedChannelBuilder.forAddress(ip, destPort).usePlaintext().build();
		RouteServiceGrpc.RouteServiceBlockingStub stub = RouteServiceGrpc.newBlockingStub(ch);
		
		var msg = this.constructMessage(msgId, destId, path, payload, originId);
		var r = stub.queryInternal(msg);
		response(r);
        ch.shutdown();
		
	}
}
