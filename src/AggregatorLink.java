import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;

/**
 * Unica connessione PERSISTENTE del nodo verso l'aggregatore.
 *
 * invia() e' synchronized e tiene il monitor per tutto lo scambio richiesta->risposta: senza,
 * due thread leggerebbero la risposta destinata all'altro sulla stessa connessione. E' l'unica
 * eccezione alla regola "niente I/O di rete sotto monitor", mitigata cosi':
 *  - timeout normale di 10 s (un aggregatore morto da' una SocketTimeoutException);
 *  - flag linkCaduto: dopo un errore di I/O ogni chiamata fallisce subito, senza retry;
 *  - nessuno chiama invia() da dentro un altro blocco synchronized.
 *
 * TIMEOUT DIFFERENZIATO: RESOLVE e RESOLVE_AT possono legittimamente restare senza risposta
 * a lungo (il token del nodo sorgente e' occupato) e le specifiche vietano che una richiesta
 * in attesa termini con errore: per questi due messaggi setSoTimeout(0) (attesa indefinita),
 * ripristinato a 10 s in un finally. Limite dichiarato: un aggregatore VIVO ma bloccato
 * non verrebbe rilevato durante quell'attesa.
 */
public class AggregatorLink {

    private static final int TIMEOUT_NORMALE_MS = 10_000;

    /** Risposta dell'aggregatore: intestazione ("OK ..." / "ERR ...") + eventuali righe. */
    public static class Risposta {
        public final String intestazione;
        public final String[] righe;

        Risposta(String intestazione, String[] righe) {
            this.intestazione = intestazione;
            this.righe = righe;
        }

        public boolean ok() {
            return intestazione.equals(Protocol.OK) || intestazione.startsWith(Protocol.OK + " ");
        }

        /** True se la risposta e' esattamente l'errore indicato (es. "ERR NOTFOUND"). */
        public boolean e(String errore) {
            return intestazione.equals(errore);
        }
    }

    private final Socket socket;
    private final InputStream in;
    private final OutputStream out;
    private boolean linkCaduto = false;          // protetto dal monitor di this

    public AggregatorLink(Socket socket) throws IOException {
        this.socket = socket;
        this.in = socket.getInputStream();
        this.out = socket.getOutputStream();
        socket.setSoTimeout(TIMEOUT_NORMALE_MS);
    }

    /**
     * Invia la richiesta (piu' eventuali righe extra, come nella REGISTER) e legge la risposta.
     * Per LIST e PEERS legge anche le n righe annunciate da "OK <n>".
     */
    public synchronized Risposta invia(String richiesta, String[] righeExtra) throws IOException {
        if (linkCaduto) {
            throw new IOException("aggregatore non piu' raggiungibile");
        }
        boolean puoAttendere = richiesta.startsWith(Protocol.RESOLVE + " ")
                || richiesta.startsWith(Protocol.RESOLVE_AT + " ");
        try {
            if (puoAttendere) {
                socket.setSoTimeout(0);                       // attesa indefinita
            }
            StringBuilder sb = new StringBuilder(richiesta).append('\n');
            if (righeExtra != null) {
                for (String r : righeExtra) {
                    sb.append(r).append('\n');
                }
            }
            out.write(sb.toString().getBytes(StandardCharsets.UTF_8));
            out.flush();

            String intestazione = Protocol.readLine(in);
            if (intestazione == null) {
                throw new EOFException("l'aggregatore ha chiuso la connessione");
            }
            String[] righe = new String[0];
            boolean elenco = richiesta.startsWith(Protocol.LIST + " ") || richiesta.equals(Protocol.PEERS);
            if (elenco && intestazione.startsWith(Protocol.OK + " ")) {
                int n = Integer.parseInt(intestazione.substring(3).trim());
                if (n < 0 || n > Protocol.MAX_RILEVAZIONI) {
                    throw new Protocol.ProtocolException("numero di righe non valido");
                }
                ArrayList<String> lette = new ArrayList<>();
                for (int i = 0; i < n; i++) {
                    String r = Protocol.readLine(in);
                    if (r == null) {
                        throw new EOFException("elenco interrotto");
                    }
                    lette.add(r);
                }
                righe = lette.toArray(new String[0]);
            }
            return new Risposta(intestazione, righe);
        } catch (IOException | NumberFormatException e) {
            linkCaduto = true;                                // stato dello stream ignoto
            if (e instanceof IOException) {
                throw (IOException) e;
            }
            throw new Protocol.ProtocolException("risposta non valida dall'aggregatore");
        } finally {
            if (puoAttendere) {
                try {
                    socket.setSoTimeout(TIMEOUT_NORMALE_MS);  // su OGNI percorso di uscita
                } catch (IOException ignorata) {
                    // socket gia' chiuso
                }
            }
        }
    }

    public synchronized boolean isCaduto() {
        return linkCaduto;
    }

    public void chiudi() {
        try {
            socket.close();
        } catch (IOException ignorata) {
            // niente da fare
        }
    }
}
