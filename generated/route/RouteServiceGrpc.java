package route;

import static io.grpc.MethodDescriptor.generateFullMethodName;

import javax.annotation.Generated;

@javax.annotation.Generated(
	    value = "by gRPC proto compiler (version 1.54.1)",
	    comments = "Source: greeting/route.proto")
	@io.grpc.stub.annotations.GrpcGenerated
	public final class RouteServiceGrpc {

	  private RouteServiceGrpc() {}

	  public static final String SERVICE_NAME = "route.RouteService";

	  // Static method descriptors that strictly reflect the proto.
	  private static volatile io.grpc.MethodDescriptor<route.Route,
	      route.Route> getRequestMethod;

	  @io.grpc.stub.annotations.RpcMethod(
	      fullMethodName = SERVICE_NAME + '/' + "request",
	      requestType = route.Route.class,
	      responseType = route.Route.class,
	      methodType = io.grpc.MethodDescriptor.MethodType.UNARY)
	  public static io.grpc.MethodDescriptor<route.Route,
	      route.Route> getRequestMethod() {
	    io.grpc.MethodDescriptor<route.Route, route.Route> getRequestMethod;
	    if ((getRequestMethod = RouteServiceGrpc.getRequestMethod) == null) {
	      synchronized (RouteServiceGrpc.class) {
	        if ((getRequestMethod = RouteServiceGrpc.getRequestMethod) == null) {
	          RouteServiceGrpc.getRequestMethod = getRequestMethod =
	              io.grpc.MethodDescriptor.<route.Route, route.Route>newBuilder()
	              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
	              .setFullMethodName(generateFullMethodName(SERVICE_NAME, "request"))
	              .setSampledToLocalTracing(true)
	              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
	                  route.Route.getDefaultInstance()))
	              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
	                  route.Route.getDefaultInstance()))
	              .setSchemaDescriptor(new RouteServiceMethodDescriptorSupplier("request"))
	              .build();
	        }
	      }
	    }
	    return getRequestMethod;
	  }

	  private static volatile io.grpc.MethodDescriptor<route.Route,
	      route.Route> getInjectMethod;

	  @io.grpc.stub.annotations.RpcMethod(
	      fullMethodName = SERVICE_NAME + '/' + "inject",
	      requestType = route.Route.class,
	      responseType = route.Route.class,
	      methodType = io.grpc.MethodDescriptor.MethodType.CLIENT_STREAMING)
	  public static io.grpc.MethodDescriptor<route.Route,
	      route.Route> getInjectMethod() {
	    io.grpc.MethodDescriptor<route.Route, route.Route> getInjectMethod;
	    if ((getInjectMethod = RouteServiceGrpc.getInjectMethod) == null) {
	      synchronized (RouteServiceGrpc.class) {
	        if ((getInjectMethod = RouteServiceGrpc.getInjectMethod) == null) {
	          RouteServiceGrpc.getInjectMethod = getInjectMethod =
	              io.grpc.MethodDescriptor.<route.Route, route.Route>newBuilder()
	              .setType(io.grpc.MethodDescriptor.MethodType.CLIENT_STREAMING)
	              .setFullMethodName(generateFullMethodName(SERVICE_NAME, "inject"))
	              .setSampledToLocalTracing(true)
	              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
	                  route.Route.getDefaultInstance()))
	              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
	                  route.Route.getDefaultInstance()))
	              .setSchemaDescriptor(new RouteServiceMethodDescriptorSupplier("inject"))
	              .build();
	        }
	      }
	    }
	    return getInjectMethod;
	  }

	  private static volatile io.grpc.MethodDescriptor<route.Route,
	      route.Route> getQueryMethod;

	  @io.grpc.stub.annotations.RpcMethod(
	      fullMethodName = SERVICE_NAME + '/' + "query",
	      requestType = route.Route.class,
	      responseType = route.Route.class,
	      methodType = io.grpc.MethodDescriptor.MethodType.SERVER_STREAMING)
	  public static io.grpc.MethodDescriptor<route.Route,
	      route.Route> getQueryMethod() {
	    io.grpc.MethodDescriptor<route.Route, route.Route> getQueryMethod;
	    if ((getQueryMethod = RouteServiceGrpc.getQueryMethod) == null) {
	      synchronized (RouteServiceGrpc.class) {
	        if ((getQueryMethod = RouteServiceGrpc.getQueryMethod) == null) {
	          RouteServiceGrpc.getQueryMethod = getQueryMethod =
	              io.grpc.MethodDescriptor.<route.Route, route.Route>newBuilder()
	              .setType(io.grpc.MethodDescriptor.MethodType.SERVER_STREAMING)
	              .setFullMethodName(generateFullMethodName(SERVICE_NAME, "query"))
	              .setSampledToLocalTracing(true)
	              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
	                  route.Route.getDefaultInstance()))
	              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
	                  route.Route.getDefaultInstance()))
	              .setSchemaDescriptor(new RouteServiceMethodDescriptorSupplier("query"))
	              .build();
	        }
	      }
	    }
	    return getQueryMethod;
	  }

	  private static volatile io.grpc.MethodDescriptor<route.Route,
	      route.Route> getQueryInternalMethod;

	  @io.grpc.stub.annotations.RpcMethod(
	      fullMethodName = SERVICE_NAME + '/' + "queryInternal",
	      requestType = route.Route.class,
	      responseType = route.Route.class,
	      methodType = io.grpc.MethodDescriptor.MethodType.UNARY)
	  public static io.grpc.MethodDescriptor<route.Route,
	      route.Route> getQueryInternalMethod() {
	    io.grpc.MethodDescriptor<route.Route, route.Route> getQueryInternalMethod;
	    if ((getQueryInternalMethod = RouteServiceGrpc.getQueryInternalMethod) == null) {
	      synchronized (RouteServiceGrpc.class) {
	        if ((getQueryInternalMethod = RouteServiceGrpc.getQueryInternalMethod) == null) {
	          RouteServiceGrpc.getQueryInternalMethod = getQueryInternalMethod =
	              io.grpc.MethodDescriptor.<route.Route, route.Route>newBuilder()
	              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
	              .setFullMethodName(generateFullMethodName(SERVICE_NAME, "queryInternal"))
	              .setSampledToLocalTracing(true)
	              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
	                  route.Route.getDefaultInstance()))
	              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
	                  route.Route.getDefaultInstance()))
	              .setSchemaDescriptor(new RouteServiceMethodDescriptorSupplier("queryInternal"))
	              .build();
	        }
	      }
	    }
	    return getQueryInternalMethod;
	  }

	  private static volatile io.grpc.MethodDescriptor<route.Route,
	      route.Route> getUploadMethod;

	  @io.grpc.stub.annotations.RpcMethod(
	      fullMethodName = SERVICE_NAME + '/' + "upload",
	      requestType = route.Route.class,
	      responseType = route.Route.class,
	      methodType = io.grpc.MethodDescriptor.MethodType.CLIENT_STREAMING)
	  public static io.grpc.MethodDescriptor<route.Route,
	      route.Route> getUploadMethod() {
	    io.grpc.MethodDescriptor<route.Route, route.Route> getUploadMethod;
	    if ((getUploadMethod = RouteServiceGrpc.getUploadMethod) == null) {
	      synchronized (RouteServiceGrpc.class) {
	        if ((getUploadMethod = RouteServiceGrpc.getUploadMethod) == null) {
	          RouteServiceGrpc.getUploadMethod = getUploadMethod =
	              io.grpc.MethodDescriptor.<route.Route, route.Route>newBuilder()
	              .setType(io.grpc.MethodDescriptor.MethodType.CLIENT_STREAMING)
	              .setFullMethodName(generateFullMethodName(SERVICE_NAME, "upload"))
	              .setSampledToLocalTracing(true)
	              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
	                  route.Route.getDefaultInstance()))
	              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
	                  route.Route.getDefaultInstance()))
	              .setSchemaDescriptor(new RouteServiceMethodDescriptorSupplier("upload"))
	              .build();
	        }
	      }
	    }
	    return getUploadMethod;
	  }

	  /**
	   * Creates a new async stub that supports all call types for the service
	   */
	  public static RouteServiceStub newStub(io.grpc.Channel channel) {
	    io.grpc.stub.AbstractStub.StubFactory<RouteServiceStub> factory =
	      new io.grpc.stub.AbstractStub.StubFactory<RouteServiceStub>() {
	        @java.lang.Override
	        public RouteServiceStub newStub(io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
	          return new RouteServiceStub(channel, callOptions);
	        }
	      };
	    return RouteServiceStub.newStub(factory, channel);
	  }

	  /**
	   * Creates a new blocking-style stub that supports unary and streaming output calls on the service
	   */
	  public static RouteServiceBlockingStub newBlockingStub(
	      io.grpc.Channel channel) {
	    io.grpc.stub.AbstractStub.StubFactory<RouteServiceBlockingStub> factory =
	      new io.grpc.stub.AbstractStub.StubFactory<RouteServiceBlockingStub>() {
	        @java.lang.Override
	        public RouteServiceBlockingStub newStub(io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
	          return new RouteServiceBlockingStub(channel, callOptions);
	        }
	      };
	    return RouteServiceBlockingStub.newStub(factory, channel);
	  }

	  /**
	   * Creates a new ListenableFuture-style stub that supports unary calls on the service
	   */
	  public static RouteServiceFutureStub newFutureStub(
	      io.grpc.Channel channel) {
	    io.grpc.stub.AbstractStub.StubFactory<RouteServiceFutureStub> factory =
	      new io.grpc.stub.AbstractStub.StubFactory<RouteServiceFutureStub>() {
	        @java.lang.Override
	        public RouteServiceFutureStub newStub(io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
	          return new RouteServiceFutureStub(channel, callOptions);
	        }
	      };
	    return RouteServiceFutureStub.newStub(factory, channel);
	  }

	  /**
	   */
	  public interface AsyncService {

	    /**
	     */
	    default void request(route.Route request,
	        io.grpc.stub.StreamObserver<route.Route> responseObserver) {
	      io.grpc.stub.ServerCalls.asyncUnimplementedUnaryCall(getRequestMethod(), responseObserver);
	    }

	    /**
	     */
	    default io.grpc.stub.StreamObserver<route.Route> inject(
	        io.grpc.stub.StreamObserver<route.Route> responseObserver) {
	      return io.grpc.stub.ServerCalls.asyncUnimplementedStreamingCall(getInjectMethod(), responseObserver);
	    }

	    /**
	     */
	    default void query(route.Route request,
	        io.grpc.stub.StreamObserver<route.Route> responseObserver) {
	      io.grpc.stub.ServerCalls.asyncUnimplementedUnaryCall(getQueryMethod(), responseObserver);
	    }

	    /**
	     */
	    default void queryInternal(route.Route request,
	        io.grpc.stub.StreamObserver<route.Route> responseObserver) {
	      io.grpc.stub.ServerCalls.asyncUnimplementedUnaryCall(getQueryInternalMethod(), responseObserver);
	    }

	    /**
	     */
	    default io.grpc.stub.StreamObserver<route.Route> upload(
	        io.grpc.stub.StreamObserver<route.Route> responseObserver) {
	      return io.grpc.stub.ServerCalls.asyncUnimplementedStreamingCall(getUploadMethod(), responseObserver);
	    }
	  }

	  /**
	   * Base class for the server implementation of the service RouteService.
	   */
	  public static abstract class RouteServiceImplBase
	      implements io.grpc.BindableService, AsyncService {

	    @java.lang.Override public final io.grpc.ServerServiceDefinition bindService() {
	      return RouteServiceGrpc.bindService(this);
	    }
	  }

	  /**
	   * A stub to allow clients to do asynchronous rpc calls to service RouteService.
	   */
	  public static final class RouteServiceStub
	      extends io.grpc.stub.AbstractAsyncStub<RouteServiceStub> {
	    private RouteServiceStub(
	        io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
	      super(channel, callOptions);
	    }

	    @java.lang.Override
	    protected RouteServiceStub build(
	        io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
	      return new RouteServiceStub(channel, callOptions);
	    }

	    /**
	     */
	    public void request(route.Route request,
	        io.grpc.stub.StreamObserver<route.Route> responseObserver) {
	      io.grpc.stub.ClientCalls.asyncUnaryCall(
	          getChannel().newCall(getRequestMethod(), getCallOptions()), request, responseObserver);
	    }

	    /**
	     */
	    public io.grpc.stub.StreamObserver<route.Route> inject(
	        io.grpc.stub.StreamObserver<route.Route> responseObserver) {
	      return io.grpc.stub.ClientCalls.asyncClientStreamingCall(
	          getChannel().newCall(getInjectMethod(), getCallOptions()), responseObserver);
	    }

	    /**
	     */
	    public void query(route.Route request,
	        io.grpc.stub.StreamObserver<route.Route> responseObserver) {
	      io.grpc.stub.ClientCalls.asyncServerStreamingCall(
	          getChannel().newCall(getQueryMethod(), getCallOptions()), request, responseObserver);
	    }

	    /**
	     */
	    public void queryInternal(route.Route request,
	        io.grpc.stub.StreamObserver<route.Route> responseObserver) {
	      io.grpc.stub.ClientCalls.asyncUnaryCall(
	          getChannel().newCall(getQueryInternalMethod(), getCallOptions()), request, responseObserver);
	    }

	    /**
	     */
	    public io.grpc.stub.StreamObserver<route.Route> upload(
	        io.grpc.stub.StreamObserver<route.Route> responseObserver) {
	      return io.grpc.stub.ClientCalls.asyncClientStreamingCall(
	          getChannel().newCall(getUploadMethod(), getCallOptions()), responseObserver);
	    }
	  }

	  /**
	   * A stub to allow clients to do synchronous rpc calls to service RouteService.
	   */
	  public static final class RouteServiceBlockingStub
	      extends io.grpc.stub.AbstractBlockingStub<RouteServiceBlockingStub> {
	    private RouteServiceBlockingStub(
	        io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
	      super(channel, callOptions);
	    }

	    @java.lang.Override
	    protected RouteServiceBlockingStub build(
	        io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
	      return new RouteServiceBlockingStub(channel, callOptions);
	    }

	    /**
	     */
	    public route.Route request(route.Route request) {
	      return io.grpc.stub.ClientCalls.blockingUnaryCall(
	          getChannel(), getRequestMethod(), getCallOptions(), request);
	    }

	    /**
	     */
	    public java.util.Iterator<route.Route> query(
	        route.Route request) {
	      return io.grpc.stub.ClientCalls.blockingServerStreamingCall(
	          getChannel(), getQueryMethod(), getCallOptions(), request);
	    }

	    /**
	     */
	    public route.Route queryInternal(route.Route request) {
	      return io.grpc.stub.ClientCalls.blockingUnaryCall(
	          getChannel(), getQueryInternalMethod(), getCallOptions(), request);
	    }
	  }

	  /**
	   * A stub to allow clients to do ListenableFuture-style rpc calls to service RouteService.
	   */
	  public static final class RouteServiceFutureStub
	      extends io.grpc.stub.AbstractFutureStub<RouteServiceFutureStub> {
	    private RouteServiceFutureStub(
	        io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
	      super(channel, callOptions);
	    }

	    @java.lang.Override
	    protected RouteServiceFutureStub build(
	        io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
	      return new RouteServiceFutureStub(channel, callOptions);
	    }

	    /**
	     */
	    public com.google.common.util.concurrent.ListenableFuture<route.Route> request(
	        route.Route request) {
	      return io.grpc.stub.ClientCalls.futureUnaryCall(
	          getChannel().newCall(getRequestMethod(), getCallOptions()), request);
	    }

	    /**
	     */
	    public com.google.common.util.concurrent.ListenableFuture<route.Route> queryInternal(
	        route.Route request) {
	      return io.grpc.stub.ClientCalls.futureUnaryCall(
	          getChannel().newCall(getQueryInternalMethod(), getCallOptions()), request);
	    }
	  }

	  private static final int METHODID_REQUEST = 0;
	  private static final int METHODID_QUERY = 1;
	  private static final int METHODID_QUERY_INTERNAL = 2;
	  private static final int METHODID_INJECT = 3;
	  private static final int METHODID_UPLOAD = 4;

	  private static final class MethodHandlers<Req, Resp> implements
	      io.grpc.stub.ServerCalls.UnaryMethod<Req, Resp>,
	      io.grpc.stub.ServerCalls.ServerStreamingMethod<Req, Resp>,
	      io.grpc.stub.ServerCalls.ClientStreamingMethod<Req, Resp>,
	      io.grpc.stub.ServerCalls.BidiStreamingMethod<Req, Resp> {
	    private final AsyncService serviceImpl;
	    private final int methodId;

	    MethodHandlers(AsyncService serviceImpl, int methodId) {
	      this.serviceImpl = serviceImpl;
	      this.methodId = methodId;
	    }

	    @java.lang.Override
	    @java.lang.SuppressWarnings("unchecked")
	    public void invoke(Req request, io.grpc.stub.StreamObserver<Resp> responseObserver) {
	      switch (methodId) {
	        case METHODID_REQUEST:
	          serviceImpl.request((route.Route) request,
	              (io.grpc.stub.StreamObserver<route.Route>) responseObserver);
	          break;
	        case METHODID_QUERY:
	          serviceImpl.query((route.Route) request,
	              (io.grpc.stub.StreamObserver<route.Route>) responseObserver);
	          break;
	        case METHODID_QUERY_INTERNAL:
	          serviceImpl.queryInternal((route.Route) request,
	              (io.grpc.stub.StreamObserver<route.Route>) responseObserver);
	          break;
	        default:
	          throw new AssertionError();
	      }
	    }

	    @java.lang.Override
	    @java.lang.SuppressWarnings("unchecked")
	    public io.grpc.stub.StreamObserver<Req> invoke(
	        io.grpc.stub.StreamObserver<Resp> responseObserver) {
	      switch (methodId) {
	        case METHODID_INJECT:
	          return (io.grpc.stub.StreamObserver<Req>) serviceImpl.inject(
	              (io.grpc.stub.StreamObserver<route.Route>) responseObserver);
	        case METHODID_UPLOAD:
	          return (io.grpc.stub.StreamObserver<Req>) serviceImpl.upload(
	              (io.grpc.stub.StreamObserver<route.Route>) responseObserver);
	        default:
	          throw new AssertionError();
	      }
	    }
	  }

	  public static final io.grpc.ServerServiceDefinition bindService(AsyncService service) {
	    return io.grpc.ServerServiceDefinition.builder(getServiceDescriptor())
	        .addMethod(
	          getRequestMethod(),
	          io.grpc.stub.ServerCalls.asyncUnaryCall(
	            new MethodHandlers<
	              route.Route,
	              route.Route>(
	                service, METHODID_REQUEST)))
	        .addMethod(
	          getInjectMethod(),
	          io.grpc.stub.ServerCalls.asyncClientStreamingCall(
	            new MethodHandlers<
	              route.Route,
	              route.Route>(
	                service, METHODID_INJECT)))
	        .addMethod(
	          getQueryMethod(),
	          io.grpc.stub.ServerCalls.asyncServerStreamingCall(
	            new MethodHandlers<
	              route.Route,
	              route.Route>(
	                service, METHODID_QUERY)))
	        .addMethod(
	          getQueryInternalMethod(),
	          io.grpc.stub.ServerCalls.asyncUnaryCall(
	            new MethodHandlers<
	              route.Route,
	              route.Route>(
	                service, METHODID_QUERY_INTERNAL)))
	        .addMethod(
	          getUploadMethod(),
	          io.grpc.stub.ServerCalls.asyncClientStreamingCall(
	            new MethodHandlers<
	              route.Route,
	              route.Route>(
	                service, METHODID_UPLOAD)))
	        .build();
	  }

	  private static abstract class RouteServiceBaseDescriptorSupplier
	      implements io.grpc.protobuf.ProtoFileDescriptorSupplier, io.grpc.protobuf.ProtoServiceDescriptorSupplier {
	    RouteServiceBaseDescriptorSupplier() {}

	    @java.lang.Override
	    public com.google.protobuf.Descriptors.FileDescriptor getFileDescriptor() {
	      return route.RouteOuterClass.getDescriptor();
	    }

	    @java.lang.Override
	    public com.google.protobuf.Descriptors.ServiceDescriptor getServiceDescriptor() {
	      return getFileDescriptor().findServiceByName("RouteService");
	    }
	  }

	  private static final class RouteServiceFileDescriptorSupplier
	      extends RouteServiceBaseDescriptorSupplier {
	    RouteServiceFileDescriptorSupplier() {}
	  }

	  private static final class RouteServiceMethodDescriptorSupplier
	      extends RouteServiceBaseDescriptorSupplier
	      implements io.grpc.protobuf.ProtoMethodDescriptorSupplier {
	    private final String methodName;

	    RouteServiceMethodDescriptorSupplier(String methodName) {
	      this.methodName = methodName;
	    }

	    @java.lang.Override
	    public com.google.protobuf.Descriptors.MethodDescriptor getMethodDescriptor() {
	      return getServiceDescriptor().findMethodByName(methodName);
	    }
	  }

	  private static volatile io.grpc.ServiceDescriptor serviceDescriptor;

	  public static io.grpc.ServiceDescriptor getServiceDescriptor() {
	    io.grpc.ServiceDescriptor result = serviceDescriptor;
	    if (result == null) {
	      synchronized (RouteServiceGrpc.class) {
	        result = serviceDescriptor;
	        if (result == null) {
	          serviceDescriptor = result = io.grpc.ServiceDescriptor.newBuilder(SERVICE_NAME)
	              .setSchemaDescriptor(new RouteServiceFileDescriptorSupplier())
	              .addMethod(getRequestMethod())
	              .addMethod(getInjectMethod())
	              .addMethod(getQueryMethod())
	              .addMethod(getQueryInternalMethod())
	              .addMethod(getUploadMethod())
	              .build();
	        }
	      }
	    }
	    return result;
	  }
	}

