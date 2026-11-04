import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;

/**
 * Thread che fa da server per gli altri peer: resta in ascolto e per ogni richiesta 
 * di download fa partire un FetchWorker. Gira in background così non blocchiamo 
 * l'input da tastiera sulla console.
 *
 * Usiamo ServerSocket(0) per far scegliere al sistema operativo una porta libera, 
 * comodo per testare più nodi in locale senza avere porte già occupate.
 */
public class NodeListener extends Thread {

    private final ServerSocket serverSocket;
    private final LocalStore store;
    private final FetchGate gate = new FetchGate();   // istanza unica condivisa tra i vari worker

    public NodeListener(LocalStore store) throws IOException {
        super("NodeListener");
        // Inizializzo qui il ServerSocket così posso leggere subito la porta da mandare nella REGISTER
        this.serverSocket = new ServerSocket(0);
        this.store = store;
    }

    public int getPort() {
        return serverSocket.getLocalPort();
    }

    // Chiudendo il socket sblocchiamo la accept() nel ciclo run, che lancerà un'eccezione
    public void chiudi() {
        try {
            serverSocket.close();
        } catch (IOException ignorata) {
        }
    }

    @Override
    public void run() {
        while (!serverSocket.isClosed()) {
            try {
                Socket socket = serverSocket.accept();
                new FetchWorker(socket, store, gate).start();
            } catch (IOException e) {
                // Se abbiamo chiuso noi il socket col quit è normale, usciamo dal while
                if (serverSocket.isClosed()) {
                    break;
                }
                System.err.println("NodeListener: errore in accept: " + e.getMessage());
            }
        }
    }
}