import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.Socket;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;

/**
 * Un thread per ogni nodo sensore connesso all'aggregatore (connessione persistente).
 * Ciclo: leggi una richiesta, eseguila, scrivi UNA risposta.
 *
 * Il NodeHandler coordina le operazioni, mentre gli oggetti di stato rimangono passivi.
 * Per evitare deadlock chiama AggregatorRegistry, PeerLockManager e DownloadLog UNO ALLA VOLTA
 * senza mai detenere due monitor contemporaneamente e 
 * senza compiere operazioni di I/O di rete mentre si mantiene un lock attivo
 */
public class NodeHandler extends Thread {

    private final Socket socket;
    private final AggregatorRegistry stato;
    private final PeerLockManager token;
    private final DownloadLog log;
    private final List<NodeHandler> handlers;

    private InputStream in;
    private OutputStream out;
    private String peerId = null;      // null finche' il nodo non ha fatto REGISTER

    public NodeHandler(Socket socket, AggregatorRegistry stato, PeerLockManager token, DownloadLog log,
                       List<NodeHandler> handlers) {
        super("NodeHandler-" + socket.getRemoteSocketAddress());
        this.socket = socket;
        this.stato = stato;
        this.token = token;
        this.log = log;
        this.handlers = handlers;
    }

    /** Chiamato dal quit dell'aggregatore: chiude il socket e sveglia un'eventuale attesa. */
    public void chiudi() {
        try {
            socket.close();
        } catch (IOException ignorata) {
            // niente da fare
        }
        interrupt();
    }

    @Override
    public void run() {
        try {
            in = socket.getInputStream();
            out = socket.getOutputStream();
            boolean continua = true;
            while (continua) {
                String riga = Protocol.readLine(in);
                if (riga == null) {
                    break;                              // EOF: il nodo e' sparito
                }
                continua = gestisci(riga);
            }
        } catch (Protocol.ProtocolException e) {
            try {
                Protocol.writeLine(out, Protocol.ERR_BADREQUEST);
            } catch (IOException ignorata) {
                // connessione gia' compromessa
            }
        } catch (IOException e) {
            // connessione interrotta: si passa alla pulizia
        } finally {
            pulisci();                                    // su OGNI percorso di uscita
        }
    }

    // Pulizia alla disconnessione (EOF, DISCONNECT, errore di I/O, quit)

    private void pulisci() {
        if (peerId != null) {
            // 1) libera i token tenuti come richiedente E quello del nodo come sorgente
            List<PeerLockManager.Sessione> chiuse = token.rilasciaTuttiDi(peerId);
            for (PeerLockManager.Sessione s : chiuse) {
                log.aggiungi(new DownloadLog.Voce(s.risorsa, s.sorgente, s.richiedente,
                        DownloadLog.FALLITO));
            }
            // 2) il peer diventa inattivo, le sue risorse restano in tabella
            stato.disconnetti(peerId);
            System.out.println("[aggregatore] " + peerId + " disconnesso.");
        }
        // 3) chiusura del socket e deregistrazione
        try {
            socket.close();
        } catch (IOException ignorata) {
            // niente da fare
        }
        synchronized (handlers) {
            handlers.remove(this);
        }
    }

    // Gestione dei messaggi. Ritorna false se la connessione va chiusa.

    private boolean gestisci(String riga) throws IOException {
        String[] t = riga.split(" ");
        String comando = t[0];

        if (peerId == null) {
            if (!comando.equals(Protocol.REGISTER)) {
                rispondi(Protocol.ERR_NOTREGISTERED);
                return true;
            }
            return gestisciRegister(t);
        }

        switch (comando) {
            case Protocol.REGISTER:
                rispondi(Protocol.ERR_BADREQUEST);          // gia' registrato
                return true;
            case Protocol.LIST:
                return gestisciList(t);
            case Protocol.PEERS:
                return gestisciPeers(t);
            case Protocol.ADDED:
                return gestisciAddedRemoved(t, true);
            case Protocol.REMOVED:
                return gestisciAddedRemoved(t, false);
            case Protocol.RESOLVE:
                return gestisciResolve(t);
            case Protocol.RESOLVE_AT:
                return gestisciResolveAt(t);
            case Protocol.RELEASE:
                return gestisciRelease(t);
            case Protocol.DISCONNECT:
                rispondi(Protocol.OK);
                return false;                               // la pulizia avviene nel finally
            default:
                rispondi(Protocol.ERR_BADREQUEST);
                return true;
        }
    }

    /**
     * Gestisce la registrazione iniziale di un nodo ricevendo il comando
     * REGISTER <porta> <n> + n righe. Le n righe si leggono FUORI da qualunque monitor 
     * e memorizzate in una lista locale; 
     * solo alla fine un'unica chiamata atomica aggiorna la tabella. 
     * Così se un client lento, morto o appeso a meta' blocca soltanto questo thread e così
     * la tabella non e' mai stata toccata e nessun monitor e' trattenuto.
     */
    private boolean gestisciRegister(String[] t) throws IOException {
        if (t.length != 3) {
            rispondi(Protocol.ERR_BADREQUEST);
            return false;
        }
        int porta;
        int n;
        try {
            porta = Integer.parseInt(t[1]);
            n = Integer.parseInt(t[2]);
        } catch (NumberFormatException e) {
            rispondi(Protocol.ERR_BADREQUEST);
            return false;
        }
        if (porta < 1 || porta > 65535 || n < 0 || n > Protocol.MAX_RILEVAZIONI) {
            rispondi(Protocol.ERR_BADREQUEST);
            return false;
        }

        LinkedHashSet<String> nomi = new LinkedHashSet<>();
        boolean valide = true;
        for (int i = 0; i < n; i++) {
            String nome = Protocol.readLine(in);
            if (nome == null) {
                throw new EOFException("REGISTER interrotta a meta'");   // tabella intatta
            }
            if (Protocol.nomeValido(nome)) {
                nomi.add(nome);
            } else {
                valide = false;
            }
        }
        if (!valide) {
            rispondi(Protocol.ERR_BADREQUEST);
            return false;
        }

        String host = socket.getInetAddress().getHostAddress();   // lo ricava l'aggregatore
        String assegnato = stato.completaRegistrazione(host, porta, new ArrayList<>(nomi));
        peerId = assegnato;
        System.out.println("[aggregatore] " + peerId + " registrato (" + host + ":" + porta + ", "
                + nomi.size() + " rilevazioni).");
        rispondi(Protocol.OK + " " + peerId);
        return true;
    }

    private boolean gestisciList(String[] t) throws IOException {
        if (t.length != 2) {
            rispondi(Protocol.ERR_BADREQUEST);
            return true;
        }
        String nome = t[1].equals(Protocol.VUOTO) ? null : t[1];
        if (nome != null && !Protocol.nomeValido(nome)) {
            rispondi(Protocol.ERR_BADREQUEST);
            return true;
        }
        List<String> righe = stato.elencoRisorse(nome);
        Protocol.writeLines(out, Protocol.OK + " " + righe.size(), righe);
        return true;
    }

    private boolean gestisciPeers(String[] t) throws IOException {
        if (t.length != 1) {
            rispondi(Protocol.ERR_BADREQUEST);
            return true;
        }
        List<String> peers = stato.elencoPeerAttivi();
        Protocol.writeLines(out, Protocol.OK + " " + peers.size(), peers);
        return true;
    }

    private boolean gestisciAddedRemoved(String[] t, boolean aggiunta) throws IOException {
        if (t.length != 2 || !Protocol.nomeValido(t[1])) {
            rispondi(Protocol.ERR_BADREQUEST);
            return true;
        }
        boolean fatto = aggiunta ? stato.aggiungiRisorsa(peerId, t[1])
                                 : stato.rimuoviRisorsa(peerId, t[1]);
        rispondi(fatto ? Protocol.OK : Protocol.ERR_BADREQUEST);
        return true;
    }

    /**
     * Gestisce la risoluzione di una risorsa ricevendo il comando 
     * RESOLVE <risorsa> <token|-> <esclusiCsv|->. Sequenza, UN MONITOR ALLA VOLTA:
     *  1) rilascio dell'eventuale token residuo;
     *  2) individua il nodo candidato sotto il monitor della tabella, che viene RILASCIATO subito;
     *  3) Se non c'è nessun candidato: registrata una voce NON_DISPONIBILE e restituisce ERR NOTFOUND, senza acquisire token;
     *  4) Se c'è un candidato: acquisizione del suo token FUORI da ogni monitor (qui si puo' attendere);
     *  5) Invio risposta al client.
     * Fra 2 e 4 la tabella puo' cambiare: non si ricontrolla perché ci pensa il retry del client.
     */
    private boolean gestisciResolve(String[] t) throws IOException {
        if (t.length != 4 || !Protocol.nomeValido(t[1])) {
            rispondi(Protocol.ERR_BADREQUEST);
            return true;
        }
        String risorsa = t[1];
        Set<String> esclusi = new HashSet<>();
        if (!t[3].equals(Protocol.VUOTO)) {
            for (String p : t[3].split(",")) {
                esclusi.add(p);
            }
        }
        if (!t[2].equals(Protocol.VUOTO)) {
            token.rilascia(t[2]);                                            // passo 1
        }
        AggregatorRegistry.EsitoRisolvi esito = stato.risolvi(peerId, risorsa, esclusi);   // passo 2
        return concludiRisoluzione(risorsa, esito);
    }

    /** RESOLVE_AT <risorsa> <peerId> <token|->: download mirato, stessa semantica di attesa. */
    private boolean gestisciResolveAt(String[] t) throws IOException {
        if (t.length != 4 || !Protocol.nomeValido(t[1]) || !Protocol.peerIdValido(t[2])) {
            rispondi(Protocol.ERR_BADREQUEST);
            return true;
        }
        String risorsa = t[1];
        if (!t[3].equals(Protocol.VUOTO)) {
            token.rilascia(t[3]);
        }
        AggregatorRegistry.EsitoRisolvi esito = stato.risolviMirato(peerId, t[2], risorsa);
        return concludiRisoluzione(risorsa, esito);
    }

    /** Passi 3-5 della RESOLVE, comuni a RESOLVE_AT. Qui NON si tiene alcun monitor. */
    private boolean concludiRisoluzione(String risorsa, AggregatorRegistry.EsitoRisolvi esito)
            throws IOException {
        if (esito.peer == null) {
            log.aggiungi(new DownloadLog.Voce(risorsa, Protocol.VUOTO, peerId,
                    DownloadLog.NON_DISPONIBILE));
            rispondi(Protocol.ERR_NOTFOUND);
            return true;
        }
        String tk;
        try {
            tk = token.acquisisci(esito.peer, peerId, risorsa);      // puo' attendere a lungo
        } catch (InterruptedException e) {
            // il token NON e' stato acquisito: non va rilasciato
            Thread.currentThread().interrupt();
            try {
                rispondi(Protocol.ERR_BADREQUEST);
            } catch (IOException ignorata) {
                // chiusura in corso
            }
            return false;
        }
        // Se questa scrittura fallisce, il finally di run() rilascia il token d'ufficio.
        rispondi(Protocol.OK + " " + tk + " " + esito.peer + " " + esito.host + " " + esito.porta);
        return true;
    }

    /**
     * Gestisce il rilascio di una risorsa ricevendo il comando
     * RELEASE <token> <OK|FAIL> <risorsa> <peerSorgente|->.
     * L'operazione procede aggiornando prima la tabella di stato e il log storico delle operazioni,
     * POI il rilascio del token, cosi' chi si risveglia vede la tabella aggiornata.
     */
    private boolean gestisciRelease(String[] t) throws IOException {
        if (t.length != 5 || !Protocol.nomeValido(t[3])
                || !(t[2].equals(Protocol.ESITO_OK) || t[2].equals(Protocol.ESITO_FAIL))) {
            rispondi(Protocol.ERR_BADREQUEST);
            return true;
        }
        PeerLockManager.Sessione s = token.leggi(t[1]);
        if (s == null || !s.richiedente.equals(peerId)) {
            rispondi(Protocol.ERR_BADTOKEN);
            return true;
        }
        if (t[2].equals(Protocol.ESITO_OK)) {
            stato.aggiungiRisorsa(peerId, s.risorsa);       // la RELEASE OK vale come notifica
            log.aggiungi(new DownloadLog.Voce(s.risorsa, s.sorgente, peerId, DownloadLog.OK));
        } else if (!t[4].equals(Protocol.VUOTO)) {
            stato.rimuoviRisorsa(s.sorgente, s.risorsa);    // eviction dell'entry fallita
            log.aggiungi(new DownloadLog.Voce(s.risorsa, s.sorgente, peerId, DownloadLog.FALLITO));
        }
        // FAIL con sorgente "-": solo rilascio, la voce NON_DISPONIBILE esiste gia'
        if (token.rilascia(t[1]) == null) {
            rispondi(Protocol.ERR_BADTOKEN);
        } else {
            rispondi(Protocol.OK);
        }
        return true;
    }

    private void rispondi(String riga) throws IOException {
        Protocol.writeLine(out, riga);
    }
}
