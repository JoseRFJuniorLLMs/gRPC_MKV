import com.tzutalin.grpc.blobkeeper.BlobKeeperGrpc;
import com.tzutalin.grpc.blobkeeper.PutRequest;
import com.tzutalin.grpc.blobkeeper.PutResponse;
import io.grpc.Server;
import io.grpc.ServerBuilder;
import io.grpc.stub.StreamObserver;

import java.io.BufferedOutputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.logging.Logger;

public class ServidorJavaGRPC {
    private static final Logger logger = Logger.getLogger(ServidorJavaGRPC.class.getName());
    private static final int PORT = 666;

    private Server mServer;

    private void start() throws IOException {

        mServer = ServerBuilder.forPort(PORT)
                .addService(new ServidorJavaGRPC.BlobKeeperImpl())
                .build()
                .start();
        logger.info("*** SERVIDOR gRPC OUVINDO A PORTA *** " + PORT);
        //Caso der Algum problema na JVM
        Runtime.getRuntime().addShutdownHook(new Thread() {
            @Override
            public void run() {
                System.err.println("*** JVM CAIU NO PROCESSO gRPC ***");
                ServidorJavaGRPC.this.stop();
                System.err.println("*** SERVIDOR CAIU ***");
            }
        });
    }

    private void stop() {
        if (mServer != null) {
            mServer.shutdown();
        }
    }

    /**
     * Espera a thread do grpc.
     */
    private void blockUntilShutdown() throws InterruptedException {
        if (mServer != null) {
            mServer.awaitTermination();
        }
    }

    /**
     * Carrega o mServer do command line.
     */
    public static void main(String[] args) throws IOException, InterruptedException {
        final ServidorJavaGRPC server = new ServidorJavaGRPC();
        server.start();
        server.blockUntilShutdown();
    }

    static class BlobKeeperImpl extends BlobKeeperGrpc.BlobKeeperImplBase {
        private int mStatus = 200;
        private String mMessage = "";
        private BufferedOutputStream mBufferedOutputStream = null;

        @Override
        public StreamObserver<PutRequest> getBlob(final StreamObserver<PutResponse> responseObserver) {
            return new StreamObserver<PutRequest>() {
                int mmCount = 0;

                @Override
                public void onNext(PutRequest request) {
                    // Print contador
                    logger.info("PROXIMO ==> PARTE ==> : " + mmCount);
                    mmCount++;

                    byte[] data = request.getData().toByteArray();
                    long offset = request.getOffset();
                    String path = "C:\\teste\\file.mkv"; //Onde vai buscar o Arquivo
                    logger.info("CAMINHO DO ARQUIVO >>"+ path );
                    String name = request.getName();
                    logger.info("NOME DO ARQUIVO >>"+ name );
                    try {
                        if (mBufferedOutputStream == null) {
                            mBufferedOutputStream = new BufferedOutputStream(new FileOutputStream(path));
                        }
                        mBufferedOutputStream.write(data);
                        mBufferedOutputStream.flush(); //Fecha depois do streaming
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                }

                @Override
                public void onError(Throwable t) {

                }

                @Override
                public void onCompleted() {
                    responseObserver.onNext(PutResponse.newBuilder().setStatus(mStatus).setMessage(mMessage).build());
                    responseObserver.onCompleted();
                    if (mBufferedOutputStream != null) {
                        try {
                            mBufferedOutputStream.close();
                        } catch (IOException e) {
                            e.printStackTrace();
                        } finally {
                            mBufferedOutputStream = null;
                        }
                    }
                }
            };
        }
    }
}
