import java.io.ByteArrayOutputStream;
import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.util.List;

/**
 * Contratto comune fra Client e Aggregator: costanti dei messaggi e primitive di I/O.
 *
 * Il protocollo e' testuale (UTF-8), una riga per messaggio terminata da '\n', campi separati
 * da UNO spazio. Ogni richiesta riceve esattamente una risposta che inizia con "OK" oppure
 * con "ERR <codice>". Il framing e' SEMPRE a lunghezza esplicita ("OK <n>" + n righe,
 * "LENGTH <n>" + n byte): mai terminatori.
 *
 * REGOLA DI CODICE: sugli stream dei socket non si usa MAI un BufferedReader, perche' il
 * suo buffer interno leggerebbe byte oltre la riga corrente e corromperebbe i payload
 * binari della FETCH. Le righe si leggono byte a byte con readLine(), i payload con
 * readFully().
 *
 * Classe senza stato: solo metodi statici puri e costanti. Nessun campo mutabile statico,
 * quindi non serve alcuna sincronizzazione (puo' essere usata da qualunque thread).
 */
public class Protocol {

    // ---- Messaggi client -> aggregatore ----
    public static final String REGISTER = "REGISTER";
    public static final String LIST = "LIST";
    public static final String PEERS = "PEERS";
    public static final String ADDED = "ADDED";
    public static final String REMOVED = "REMOVED";
    public static final String RESOLVE = "RESOLVE";
    public static final String RESOLVE_AT = "RESOLVE_AT";
    public static final String RELEASE = "RELEASE";
    public static final String DISCONNECT = "DISCONNECT";

    // ---- Messaggio nodo -> nodo ----
    public static final String FETCH = "FETCH";
    public static final String LENGTH = "LENGTH";

    // ---- Risposte ----
    public static final String OK = "OK";
    public static final String ERR_BADREQUEST = "ERR BADREQUEST";
    public static final String ERR_NOTREGISTERED = "ERR NOTREGISTERED";
    public static final String ERR_NOTFOUND = "ERR NOTFOUND";
    public static final String ERR_BADTOKEN = "ERR BADTOKEN";

    /** Valore "vuoto" usato nei campi opzionali (token assente, nessun escluso, nessuna sorgente). */
    public static final String VUOTO = "-";

    // ---- Esiti di una RELEASE ----
    public static final String ESITO_OK = "OK";
    public static final String ESITO_FAIL = "FAIL";

    // ---- Limiti difensivi ----
    public static final int MAX_RIGA = 8 * 1024;               // 8 KB per una riga di controllo
    public static final int MAX_LENGTH = 10 * 1024 * 1024;     // 10 MB per un payload
    public static final int MAX_RILEVAZIONI = 100_000;         // massimo n in una REGISTER

    private Protocol() { }

    /** Errore di formato: il mittente ha violato il protocollo (es. riga troppo lunga). */
    public static class ProtocolException extends IOException {
        private static final long serialVersionUID = 1L;

        public ProtocolException(String messaggio) {
            super(messaggio);
        }
    }

    // ------------------------------------------------------------------------------------
    // I/O
    // ------------------------------------------------------------------------------------

    /**
     * Legge una riga byte a byte fino a '\n' (l'eventuale '\r' finale viene scartato).
     * Ritorna null se lo stream e' chiuso (EOF) prima di completare la riga: chi legge una
     * richiesta lo interpreta come disconnessione, chi legge una risposta come fallimento.
     */
    public static String readLine(InputStream in) throws IOException {
        ByteArrayOutputStream buffer = new ByteArrayOutputStream();
        int b;
        while ((b = in.read()) != -1) {
            if (b == '\n') {
                byte[] grezzo = buffer.toByteArray();
                int lunghezza = grezzo.length;
                if (lunghezza > 0 && grezzo[lunghezza - 1] == '\r') {
                    lunghezza--;
                }
                return new String(grezzo, 0, lunghezza, StandardCharsets.UTF_8);
            }
            if (buffer.size() >= MAX_RIGA) {
                throw new ProtocolException("riga di controllo troppo lunga");
            }
            buffer.write(b);
        }
        return null;
    }

    /** Scrive una riga (aggiungendo '\n') con una sola write e fa il flush. */
    public static void writeLine(OutputStream out, String riga) throws IOException {
        out.write((riga + "\n").getBytes(StandardCharsets.UTF_8));
        out.flush();
    }

    /** Scrive un'intestazione seguita da n righe, in un'unica write (messaggio non spezzato). */
    public static void writeLines(OutputStream out, String intestazione, List<String> righe)
            throws IOException {
        StringBuilder sb = new StringBuilder(intestazione).append('\n');
        for (String r : righe) {
            sb.append(r).append('\n');
        }
        out.write(sb.toString().getBytes(StandardCharsets.UTF_8));
        out.flush();
    }

    /** Legge esattamente n byte; solleva EOFException se lo stream finisce prima. */
    public static byte[] readFully(InputStream in, int n) throws IOException {
        byte[] dati = new byte[n];
        int letti = 0;
        while (letti < n) {
            int r = in.read(dati, letti, n - letti);
            if (r < 0) {
                throw new EOFException("attesi " + n + " byte, ricevuti solo " + letti);
            }
            letti += r;
        }
        return dati;
    }

    // ------------------------------------------------------------------------------------
    // Validazione e formattazione
    // ------------------------------------------------------------------------------------

    /** Un nome di rilevazione e' senza spazi e sicuro come nome di file (&lt;nome&gt;.txt). */
    public static boolean nomeValido(String nome) {
        return nome != null
                && nome.matches("[A-Za-z0-9_-][A-Za-z0-9_.-]*")
                && !nome.contains("..")
                && nome.length() <= 100;
    }

    /** Un identificativo di nodo ha la forma peer0, peer1, ... */
    public static boolean peerIdValido(String peerId) {
        return peerId != null && peerId.matches("peer[0-9]{1,9}");
    }

    /**
     * Trasforma una riga del protocollo "temp_bo peer0 peer1" nel formato richiesto
     * dalle specifiche per la console: "temp_bo: peer0, peer1".
     */
    public static String formattaRiga(String riga) {
        String[] campi = riga.split(" ");
        StringBuilder sb = new StringBuilder(campi[0]).append(':');
        for (int i = 1; i < campi.length; i++) {
            sb.append(i == 1 ? " " : ", ").append(campi[i]);
        }
        return sb.toString();
    }
}