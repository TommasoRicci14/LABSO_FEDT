import java.time.LocalTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.List;

/**
 * Registro che tiene traccia di tutte le richieste di download effettuate,
 * utile per mostrare la cronologia quando viene digitato il comando "log" dell'aggregatore.
 *
 * È una risorsa condivisa dai NodeHandler (N thread), letta dalla console.
 * Utilizza metodi synchronized per garantire la mutua esclusione:
 * i thread accedono ai dati uno alla volta per evitare conflitti, senza l'uso di wait().
 * la sincronizzazione di questa classe è completamente indipendente da 
 * quella della tabella, così la scrittura dei log non blocca mai le operazioni 
 * sui dati e viceversa.
 */
public class DownloadLog {

    public static final String OK = "OK";
    public static final String FALLITO = "FALLITO";
    public static final String NON_DISPONIBILE = "NON_DISPONIBILE";

    private static final DateTimeFormatter ORARIO = DateTimeFormatter.ofPattern("HH:mm:ss");

    /** Una voce del log; immutabile, l'orario e' fissato alla creazione. */
    public static class Voce {
        private final String ora;
        private final String risorsa;
        private final String da;      // peer sorgente, oppure "-" se non c'era un candidato
        private final String a;       // peer richiedente
        private final String esito;

        public Voce(String risorsa, String da, String a, String esito) {
            this.ora = LocalTime.now().format(ORARIO);
            this.risorsa = risorsa;
            this.da = da;
            this.a = a;
            this.esito = esito;
        }

        String formatta() {
            return "- " + ora + " " + risorsa + " da: " + da + " a: " + a + " esito: " + esito;
        }
    }

    private final ArrayList<Voce> voci = new ArrayList<>();

    public synchronized void aggiungi(Voce voce) {
        voci.add(voce);
    }

    /** Restituisce una copia formattata delle voci, proteggendo la lista interna da modifiche esterne. */
    public synchronized List<String> righe() {
        ArrayList<String> copia = new ArrayList<>();
        for (Voce v : voci) {
            copia.add(v.formatta());
        }
        return copia;
    }
}
