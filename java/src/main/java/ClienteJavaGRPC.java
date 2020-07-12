import com.google.protobuf.ByteString;
import com.tzutalin.grpc.blobkeeper.BlobKeeperGrpc;
import com.tzutalin.grpc.blobkeeper.PutRequest;
import com.tzutalin.grpc.blobkeeper.PutResponse;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.stub.StreamObserver;

import java.io.*;
import java.util.concurrent.TimeUnit;
import java.util.logging.Logger;

public class ClienteJavaGRPC {
    private static final Logger logger = Logger.getLogger(ClienteJavaGRPC.class.getName());
    private static final int PORT = 666;

    private final ManagedChannel mChannel;
    private final BlobKeeperGrpc.BlobKeeperBlockingStub mBlockingStub;
    private final BlobKeeperGrpc.BlobKeeperStub mAsyncStub;

    public ClienteJavaGRPC(String host, int port) {
        this(ManagedChannelBuilder.forAddress(host, port)
                .usePlaintext(true)
                .build());
    }

    ClienteJavaGRPC(ManagedChannel channel) {
        this.mChannel = channel;
        mBlockingStub = BlobKeeperGrpc.newBlockingStub(channel);
        mAsyncStub = BlobKeeperGrpc.newStub(channel);
    }

    public void shutdown() throws InterruptedException {
        mChannel.shutdown().awaitTermination(5, TimeUnit.SECONDS);
    }

    public void startStream(final String filepath) {
        logger.info("tid: " +  Thread.currentThread().getId() + ", *** TENTANDO NOVAMENTE getBlob ***");
        StreamObserver<PutResponse> responseObserver = new StreamObserver<PutResponse>() {

            @Override
            public void onNext(PutResponse value) {
                logger.info("*** SERVIDOR RESPONDE onNext ***");
            }

            @Override
            public void onError(Throwable t) {
                logger.info("*** SERVIDOR RESPONDE ERRO ***");
            }

            @Override
            public void onCompleted() {
                logger.info("*** SERVIDOR RESPONDE OK TERMINADO ***");
            }
        };
        StreamObserver<PutRequest> requestObserver = mAsyncStub.getBlob(responseObserver);
        try {
            File file = new File(filepath);
            if (file.exists() == false) {
                logger.info("*** ARQUIVO NAO EXISTE NAO CONSIGO CONECTAR ***");
                return;
            }
            try {
                BufferedInputStream bInputStream = new BufferedInputStream(new FileInputStream(file));
                int bufferSize = 1024 * 1024; // 1M testar outros tamanhos
                byte[] buffer = new byte[bufferSize];                
                int size = 0;
                while ((size = bInputStream.read(buffer)) > 0) {                    
                    ByteString byteString = ByteString.copyFrom(buffer, 0, size);
                    PutRequest req = PutRequest.newBuilder().setName(filepath).setData(byteString).setOffset(size).build();
                    requestObserver.onNext(req);
                }
            } catch (FileNotFoundException e) {
                e.printStackTrace();
            } catch (IOException e) {
                e.printStackTrace();
            }
        } catch (RuntimeException e) {
            requestObserver.onError(e);
            throw e;
        }
        requestObserver.onCompleted();
    }

    public static void main(String[] args) throws Exception {
        ClienteJavaGRPC client = new ClienteJavaGRPC("localhost", PORT);
        try {
            client.startStream("D:\\teste\\file.mkv");
            logger.info("*** Terminado com startStream ***");
        } finally {
            try {
                Thread.sleep(500);
                logger.info("*** PAUSANDO 500 Mls ***");
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            client.shutdown();
        }
    }
}
