import java.time.LocalTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.List;

/**
 * Registro che tiene traccia di tutte le richieste di download effettuate,
 * utile per mostrare la cronologia quando viene digitato il comando "log" dell'aggregatore.
 *
 * Risorsa condivisa: scritta dai NodeHandler (N thread), letta dalla console.
 * Monitor: l'istanza stessa. Nessun wait(): solo mutua esclusione. Il suo monitor e'
 * indipendente da quello della tabella: scrivere una riga di log non deve mai far attendere
 * una listdata, e viceversa.
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

    /** Copia formattata delle voci: nessun riferimento interno esce dal monitor. */
    public synchronized List<String> righe() {
        ArrayList<String> copia = new ArrayList<>();
        for (Voce v : voci) {
            copia.add(v.formatta());
        }
        return copia;
    }
}
