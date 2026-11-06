import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.Socket;
import java.nio.charset.StandardCharsets;

/**
 * Thread che gestisce una singola richiesta FETCH da un altro peer e poi chiude il socket.
 *
 * Per la sincronizzazione con FetchGate:
 * 1. Chiamiamo gate.acquisisci() fuori dal try principale: se va in InterruptedException non abbiamo preso il lock.
 * 2. Nel try facciamo l'invio dei byte senza tenere nessun blocco synchronized.
 * 3. Nel finally rilasciamo il gate, sicuri di averlo effettivamente acquisito prima.
 */
public class FetchWorker extends Thread {

    private static final int TIMEOUT_MS = 10_000;

    private final Socket socket;
    private final LocalStore store;
    private final FetchGate gate;

    public FetchWorker(Socket socket, LocalStore store, FetchGate gate) {
        super("FetchWorker-" + socket.getRemoteSocketAddress());
        this.socket = socket;
        this.store = store;
        this.gate = gate;
    }

    @Override
    public void run() {
        String etichetta = String.valueOf(socket.getRemoteSocketAddress());
        try {
            // Timeout per evitare che un client che si connette senza mandare nulla blocchi il nodo all'infinito
            socket.setSoTimeout(TIMEOUT_MS);

            if (gate.isOccupato()) {
                System.out.println("[" + etichetta + "] in attesa (in servizio: "
                        + gate.getTitolare() + ", in coda: " + gate.getInAttesa() + ")...");
            }
            try {
                gate.acquisisci(etichetta);
            } catch (InterruptedException e) {
                // Se veniamo interrotti nella wait non abbiamo il lock, quindi usciamo senza fare rilascia()
                Thread.currentThread().interrupt();
                return;
            }
            try {
                servi(etichetta);
            } finally {
                gate.rilascia();
            }
        } catch (IOException e) {
            System.out.println("[" + etichetta + "] richiesta interrotta: " + e.getMessage());
        } finally {
            // Chiudiamo sempre la connessione prima di terminare il thread
            try {
                socket.close();
            } catch (IOException ignorata) {
            }
        }
    }

    // Legge il comando FETCH, recupera il file dal LocalStore e invia i byte al peer richiedente
    private void servi(String etichetta) throws IOException {
        InputStream in = socket.getInputStream();
        OutputStream out = socket.getOutputStream();

        // Formato atteso: FETCH <nome> <token>. Se readLine ritorna null il peer ha chiuso la connessione
        String riga = Protocol.readLine(in);
        if (riga == null) {
            return;
        }
        String[] t = riga.split(" ");
        if (t.length != 3 || !t[0].equals(Protocol.FETCH) || !Protocol.nomeValido(t[1])) {
            Protocol.writeLine(out, Protocol.ERR_BADREQUEST);
            return;
        }
        System.out.println("[" + etichetta + "] servo FETCH " + t[1] + " (token " + t[2] + ")");

        byte[] contenuto;
        try {
            contenuto = store.leggi(t[1]);
        } catch (IOException e) {
            contenuto = null;       // Se c'è un errore di lettura su disco facciamo finta che non ci sia
        }
        if (contenuto == null) {
            Protocol.writeLine(out, Protocol.ERR_NOTFOUND);
            System.out.println("[" + etichetta + "] completata (rilevazione non trovata)");
            return;
        }

        // Mandiamo l'header con OK e LENGTH <dimensione>, seguito subito dai byte del file
        String intestazione = Protocol.OK + "\n" + Protocol.LENGTH + " " + contenuto.length + "\n";
        out.write(intestazione.getBytes(StandardCharsets.UTF_8));
        out.write(contenuto);
        out.flush();
        System.out.println("[" + etichetta + "] completata (" + contenuto.length + " byte)");
    }
}