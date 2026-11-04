import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.util.ArrayList;

/**
 * Gestisce la parte client dei download occupandosi del giro RESOLVE -> FETCH -> RELEASE
 * (ed eventuali retry). Gira direttamente sul thread della console: finché scarica il prompt 
 * aspetta, ma intanto il NodeListener in background continua a rispondere agli altri peer.
 *
 * Nota importante sui token: se un tentativo va male, rilasciamo subito il token del nodo
 * sorgente PRIMA di chiedere un altro peer all'aggregatore, altrimenti ci terremmo bloccati
 * i permessi di tutti i nodi falliti lungo la strada.
 *
 * Usiamo l'interfaccia Collegamento per parlare con l'aggregatore senza legarci per forza
 * ad AggregatorLink, mentre i file scaricati finiscono nel LocalStore.
 */
public class RetrievalCoordinator {

    // Interfaccia funzionale per astrarre la comunicazione con l'aggregatore (passata via lambda nel Client).
    // Manda il comando testuale e ritorna la riga di risposta (es. "OK t0 peer1 ...", "ERR NOTFOUND").
    public interface Collegamento {
        String invia(String richiesta) throws IOException;
    }

    private static final int MAX_TENTATIVI = 50;     // limite per evitare loop infiniti
    private static final int TIMEOUT_CONNESSIONE_MS = 5_000;
    private static final int TIMEOUT_LETTURA_MS = 10_000;

    // Struttura di comodo per impacchettare il risultato di una FETCH
    public static class Esito {
        boolean riuscito;
        boolean fatale;        // se true è un problema nostro locale, inutile fare retry su altri peer
        byte[] contenuto;
        String motivo;

        static Esito ok(byte[] contenuto) {
            Esito e = new Esito();
            e.riuscito = true;
            e.contenuto = contenuto;
            return e;
        }

        static Esito fallito(String motivo) {
            Esito e = new Esito();
            e.motivo = motivo;
            return e;
        }
    }

    private final String mioPeerId;
    private final Collegamento aggregatore;
    private final LocalStore archivio;

    public RetrievalCoordinator(String mioPeerId, Collegamento aggregatore, LocalStore archivio) {
        this.mioPeerId = mioPeerId;
        this.aggregatore = aggregatore;
        this.archivio = archivio;
    }

    // Download automatico: l'aggregatore sceglie il peer e noi facciamo retry se qualcuno cade
    public void scarica(String nome) {
        if (archivio.contiene(nome)) {
            System.out.println("Rilevazione '" + nome + "' già presente in locale.");
            return;
        }
        ArrayList<String> esclusi = new ArrayList<>();   // lista dei peer già provati e andati male
        String token = Protocol.VUOTO;
        int tentativi = 0;
        try {
            while (true) {
                tentativi++;
                if (tentativi > MAX_TENTATIVI) {
                    System.out.println("Troppi tentativi, download interrotto.");
                    return;
                }
                // Se il peer scelto è occupato, l'aggregatore ci mette in wait e risponde
                // solo quando si libera il token, quindi qui può bloccarsi un po' ed è normale.
                String r = aggregatore.invia(Protocol.RESOLVE + " " + nome + " " + token
                        + " " + csv(esclusi));

                if (r.equals(Protocol.ERR_NOTFOUND)) {
                    // Se avevamo un token appeso lo liberiamo prima di uscire
                    if (!token.equals(Protocol.VUOTO)) {
                        aggregatore.invia(rilascio(token, Protocol.ESITO_FAIL, nome, Protocol.VUOTO));
                    }
                    System.out.println("Rilevazione '" + nome + "' non disponibile sulla rete.");
                    return;
                }
                if (!isOk(r)) {
                    System.out.println("Errore dall'aggregatore: " + r);
                    return;
                }
                // Formato: OK <token> <peerId> <host> <porta>
                String[] p = r.split(" ");
                int porta = leggiPorta(p);
                if (porta < 0) {
                    System.out.println("Errore: risposta non valida dall'aggregatore.");
                    return;
                }
                token = p[1];
                String peer = p[2];
                // Da questo momento abbiamo in mano il token per 'peer', va rilasciato comunque vada

                Esito e = tentaTrasferimento(token, peer, p[3], porta, nome);
                if (e.riuscito) {
                    System.out.println("Scaricata '" + nome + "' da " + peer + " ("
                            + e.contenuto.length + " byte).");
                    return;
                }
                if (e.fatale) {
                    System.out.println("Errore: download interrotto (" + e.motivo + ").");
                    return;
                }
                System.out.println("Tentativo su " + peer + " fallito (" + e.motivo
                        + "), riprovo...");
                token = Protocol.VUOTO;      // dentro tentaTrasferimento abbiamo già mandato la RELEASE FAIL
                esclusi.add(peer);
            }
        } catch (IOException ex) {
            System.out.println("Errore: aggregatore non più raggiungibile.");
        }
    }

    // Download mirato da un peer specifico: fa un tentativo secco senza retry
    public void scaricaDa(String peerScelto, String nome) {
        if (archivio.contiene(nome)) {
            System.out.println("Rilevazione '" + nome + "' già presente in locale.");
            return;
        }
        if (peerScelto.equals(mioPeerId)) {
            System.out.println("Errore: non puoi scaricare da te stesso.");
            return;
        }
        try {
            String r = aggregatore.invia(Protocol.RESOLVE_AT + " " + nome + " " + peerScelto
                    + " " + Protocol.VUOTO);
            if (r.equals(Protocol.ERR_NOTFOUND)) {
                System.out.println("Rilevazione '" + nome + "' non disponibile su "
                        + peerScelto + ".");
                return;
            }
            if (!isOk(r)) {
                System.out.println("Errore dall'aggregatore: " + r);
                return;
            }
            String[] p = r.split(" ");
            int porta = leggiPorta(p);
            if (porta < 0) {
                System.out.println("Errore: risposta non valida dall'aggregatore.");
                return;
            }
            Esito e = tentaTrasferimento(p[1], p[2], p[3], porta, nome);
            if (e.riuscito) {
                System.out.println("Scaricata '" + nome + "' da " + p[2] + " ("
                        + e.contenuto.length + " byte).");
            } else {
                System.out.println("Download da " + p[2] + " fallito (" + e.motivo + ").");
            }
        } catch (IOException ex) {
            System.out.println("Errore: aggregatore non più raggiungibile.");
        }
    }

    /**
     * Esegue il download vero e proprio dal peer, salva nel LocalStore e manda sempre
     * la RELEASE all'aggregatore (OK o FAIL) per non lasciare il nodo sorgente bloccato.
     */
    private Esito tentaTrasferimento(String token, String peer, String host, int porta,
                                     String nome) throws IOException {
        Esito e = provaFetch(host, porta, nome, token);

        if (!e.riuscito) {
            // Passiamo il nome del peer così l'aggregatore sa che ha fallito e aggiorna la tabella
            aggregatore.invia(rilascio(token, Protocol.ESITO_FAIL, nome, peer));
            return e;
        }

        // File scaricato, proviamo a salvarlo in locale
        boolean salvata;
        try {
            salvata = archivio.aggiungi(nome, e.contenuto);
        } catch (IOException ex) {
            // Errore nostro di scrittura: mandiamo FAIL ma con "-" per non penalizzare il peer sorgente
            aggregatore.invia(rilascio(token, Protocol.ESITO_FAIL, nome, Protocol.VUOTO));
            Esito f = Esito.fallito("scrittura locale: " + ex.getMessage());
            f.fatale = true;
            return f;
        }
        if (!salvata) {
            aggregatore.invia(rilascio(token, Protocol.ESITO_FAIL, nome, Protocol.VUOTO));
            Esito f = Esito.fallito("rilevazione già presente in locale");
            f.fatale = true;
            return f;
        }

        // Con RELEASE OK liberiamo il token e diciamo all'aggregatore che ora abbiamo anche noi il file
        String r = aggregatore.invia(rilascio(token, Protocol.ESITO_OK, nome, peer));
        if (r.equals(Protocol.ERR_BADTOKEN)) {
            // Caso limite: se il token era scaduto/invalidato nel frattempo, notifichiamo comunque il file con ADDED
            aggregatore.invia(Protocol.ADDED + " " + nome);
        }
        return e;
    }

    // Apre il socket verso il peer e scarica i byte. Non rilancia eccezioni ma restituisce un oggetto Esito.
    public Esito provaFetch(String host, int porta, String nome, String token) {
        try (Socket socket = new Socket()) {
            socket.connect(new InetSocketAddress(host, porta), TIMEOUT_CONNESSIONE_MS);
            socket.setSoTimeout(TIMEOUT_LETTURA_MS);

            Protocol.writeLine(socket.getOutputStream(),
                    Protocol.FETCH + " " + nome + " " + token);

            String riga = Protocol.readLine(socket.getInputStream());
            if (riga == null) {
                return Esito.fallito("connessione chiusa dal nodo");
            }
            if (!riga.equals(Protocol.OK)) {
                return Esito.fallito("risposta del nodo: " + riga);
            }

            // Leggiamo "LENGTH <n>" per sapere quanti byte aspettarci
            String rigaLen = Protocol.readLine(socket.getInputStream());
            if (rigaLen == null || !rigaLen.startsWith(Protocol.LENGTH + " ")) {
                return Esito.fallito("framing non valido");
            }
            int n = Integer.parseInt(rigaLen.substring(Protocol.LENGTH.length() + 1).trim());
            if (n < 0 || n > Protocol.MAX_LENGTH) {
                return Esito.fallito("framing non valido");
            }

            // Legge esattamente n byte (se cade la connessione a metà lancia EOFException e scarta tutto)
            byte[] dati = Protocol.readFully(socket.getInputStream(), n);
            return Esito.ok(dati);

        } catch (IOException | IllegalArgumentException ex) {
            // Cattura sia problemi di rete/timeout che errori nel parsing di LENGTH
            return Esito.fallito(String.valueOf(ex.getMessage()));
        }
    }

    // --- Metodi helper per formattare e parsare le stringhe del protocollo ---

    private static String rilascio(String token, String esito, String nome, String sorgente) {
        return Protocol.RELEASE + " " + token + " " + esito + " " + nome + " " + sorgente;
    }

    // Converte la lista dei peer esclusi in formato "peer1,peer2" (o "-" se vuota)
    private static String csv(ArrayList<String> peers) {
        if (peers.isEmpty()) {
            return Protocol.VUOTO;
        }
        return String.join(",", peers);
    }

    private static boolean isOk(String intestazione) {
        return intestazione.equals(Protocol.OK) || intestazione.startsWith(Protocol.OK + " ");
    }

    private static int leggiPorta(String[] p) {
        if (p.length != 5) {
            return -1;
        }
        try {
            int porta = Integer.parseInt(p[4]);
            return (porta >= 1 && porta <= 65535) ? porta : -1;
        } catch (NumberFormatException e) {
            return -1;
        }
    }
}