import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * Archivio locale delle rilevazioni di un nodo: ogni rilevazione e' un file <nome>.txt nella
 * cartella dati del nodo, piu' un indice dei nomi tenuto in memoria (ordinato).
 *
 * Risorsa condivisa: usata dalla console, dal RetrievalCoordinator (stesso thread) e dai
 * FetchWorker (N thread). Monitor: l'istanza. Nessun wait(): solo mutua esclusione.
 *
 * L'I/O su file LOCALE avviene dentro il monitor: e' una deroga consapevole alla regola
 * "niente I/O sotto monitor", che riguarda l'I/O di RETE (durata illimitata, dipende da una
 * controparte remota). Un file locale di pochi KB e' breve e limitato, e tenerlo nel monitor
 * garantisce che indice e contenuto non divergano. Costo: una listdata local puo' attendere
 * la lettura di un file grande.
 */
public class LocalStore {

    private static final String ESTENSIONE = ".txt";

    private final File cartella;
    private final ArrayList<String> nomi = new ArrayList<>();   // indice, protetto dal monitor

    /** Legge la cartella e costruisce l'indice (chiamato dal main, prima di altri thread). */
    public LocalStore(File cartella) {
        this.cartella = cartella;
        File[] file = cartella.listFiles();
        if (file != null) {
            for (File f : file) {
                String n = f.getName();
                if (f.isFile() && n.endsWith(ESTENSIONE)) {
                    String nome = n.substring(0, n.length() - ESTENSIONE.length());
                    if (Protocol.nomeValido(nome)) {
                        nomi.add(nome);
                    }
                }
            }
        }
        Collections.sort(nomi);
    }

    /** Copia ordinata dell'indice. */
    public synchronized List<String> elenco() {
        return new ArrayList<>(nomi);
    }

    public synchronized boolean contiene(String nome) {
        return nomi.contains(nome);
    }

    /** Contenuto della rilevazione, o null se non presente. */
    public synchronized byte[] leggi(String nome) throws IOException {
        if (!nomi.contains(nome)) {
            return null;
        }
        try {
            return Files.readAllBytes(file(nome).toPath());
        } catch (NoSuchFileException e) {
            nomi.remove(nome);          // il file e' sparito dal disco: riallineo l'indice
            return null;
        }
    }

    /** Scrive il file e aggiorna l'indice. Ritorna false se il nome e' gia' presente. */
    public synchronized boolean aggiungi(String nome, byte[] contenuto) throws IOException {
        if (nomi.contains(nome)) {
            return false;
        }
        Files.write(file(nome).toPath(), contenuto);
        nomi.add(nome);
        Collections.sort(nomi);
        return true;
    }

    /** Elimina file e voce d'indice. Ritorna false se il nome non era presente. */
    public synchronized boolean rimuovi(String nome) throws IOException {
        if (!nomi.contains(nome)) {
            return false;
        }
        Files.deleteIfExists(file(nome).toPath());
        nomi.remove(nome);
        return true;
    }

    private File file(String nome) {
        return new File(cartella, nome + ESTENSIONE);
    }
}
