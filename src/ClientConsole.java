import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.List;

/**
 * Console interattiva del nodo sensore (gira sul thread main).
 * Comandi richiesti: listdata local | listdata remote | add <nome> <contenuto> |
 * download <nome> | quit. Estensioni (si AGGIUNGONO, non modificano i comandi richiesti):
 * listdata remote <nome>, listdata peers, download <peer> <nome>, remove <nome>, help.
 *
 * I formati di output richiesti dalle specifiche sono riprodotti alla lettera.
 */
public class ClientConsole {

    private final String peerId;
    private final AggregatorLink link;
    private final LocalStore store;
    private final DownloadManager download;
    private final PeerServer peerServer;

    public ClientConsole(String peerId, AggregatorLink link, LocalStore store,
                         DownloadManager download, PeerServer peerServer) {
        this.peerId = peerId;
        this.link = link;
        this.store = store;
        this.download = download;
        this.peerServer = peerServer;
    }

    public void esegui() {
        BufferedReader tastiera = new BufferedReader(new InputStreamReader(System.in));
        try {
            while (true) {
                System.out.print("> ");
                System.out.flush();
                String riga = tastiera.readLine();
                if (riga == null) {
                    esci();                                   // EOF (Ctrl-D) = quit
                    return;
                }
                riga = riga.trim();
                if (riga.isEmpty()) {
                    continue;
                }
                if (!gestisci(riga)) {
                    return;
                }
            }
        } catch (IOException e) {
            esci();
        }
    }

    /** Ritorna false dopo un quit. */
    private boolean gestisci(String riga) {
        String[] t = riga.split("\\s+", 3);
        String comando = t[0];
        switch (comando) {
            case "listdata":
                listdata(t);
                return true;
            case "add":
                add(t);
                return true;
            case "remove":
                remove(t);
                return true;
            case "download":
                download(riga.split("\\s+"));
                return true;
            case "quit":
                esci();
                return false;
            case "help":
                stampaHelp();
                return true;
            default:
                System.out.println("Comando non riconosciuto. Digita 'help'.");
                return true;
        }
    }

    private void listdata(String[] t) {
        if (t.length < 2) {
            System.out.println("Uso: listdata local | listdata remote [<nome>] | listdata peers");
            return;
        }
        switch (t[1]) {
            case "local":
                System.out.println("Risorse:");
                for (String nome : store.elenco()) {
                    System.out.println("- " + nome);
                }
                break;
            case "remote":
                listdataRemote(t.length == 3 ? t[2].trim() : null);
                break;
            case "peers":
                listdataPeers();
                break;
            default:
                System.out.println("Uso: listdata local | listdata remote [<nome>] | listdata peers");
        }
    }

    private void listdataRemote(String nome) {
        if (nome != null && !Protocol.nomeValido(nome)) {
            System.out.println("Errore: nome di rilevazione non valido.");
            return;
        }
        try {
            AggregatorLink.Risposta r = link.invia(Protocol.LIST + " "
                    + (nome == null ? Protocol.VUOTO : nome), null);
            if (!r.ok()) {
                System.out.println("Errore dall'aggregatore: " + r.intestazione);
                return;
            }
            System.out.println("Risorse:");
            for (String riga : r.righe) {
                System.out.println("- " + Protocol.formattaRiga(riga));
            }
        } catch (IOException e) {
            erroreAggregatore();
        }
    }

    private void listdataPeers() {
        try {
            AggregatorLink.Risposta r = link.invia(Protocol.PEERS, null);
            if (!r.ok()) {
                System.out.println("Errore dall'aggregatore: " + r.intestazione);
                return;
            }
            System.out.println("Nodi attivi:");
            for (String p : r.righe) {
                if (!p.equals(peerId)) {                  // gli *altri* nodi
                    System.out.println("- " + p);
                }
            }
        } catch (IOException e) {
            erroreAggregatore();
        }
    }

    /** add <nome> <contenuto>: il contenuto si prende fino a fine riga. */
    private void add(String[] t) {
        if (t.length < 3 || t[2].trim().isEmpty()) {
            System.out.println("Uso: add <nome> <contenuto>");
            return;
        }
        String nome = t[1];
        if (!Protocol.nomeValido(nome)) {
            System.out.println("Errore: nome di rilevazione non valido (lettere, cifre, _ - .).");
            return;
        }
        try {
            if (!store.aggiungi(nome, t[2].getBytes(StandardCharsets.UTF_8))) {
                System.out.println("Errore: rilevazione '" + nome + "' già presente.");
                return;
            }
        } catch (IOException e) {
            System.out.println("Errore: impossibile scrivere la rilevazione: " + e.getMessage());
            return;
        }
        try {
            AggregatorLink.Risposta r = link.invia(Protocol.ADDED + " " + nome, null);
            if (r.ok()) {
                System.out.println("Rilevazione '" + nome + "' aggiunta e notificata all'aggregatore.");
            } else {
                System.out.println("Rilevazione '" + nome + "' aggiunta in locale, ma l'aggregatore ha risposto: "
                        + r.intestazione);
            }
        } catch (IOException e) {
            System.out.println("Rilevazione '" + nome + "' aggiunta in locale, ma l'aggregatore non è raggiungibile.");
        }
    }

    /** remove <nome>: estensione, serve a dimostrare il download robusto. */
    private void remove(String[] t) {
        if (t.length != 2) {
            System.out.println("Uso: remove <nome>");
            return;
        }
        String nome = t[1];
        try {
            if (!store.rimuovi(nome)) {
                System.out.println("Errore: rilevazione '" + nome + "' non presente.");
                return;
            }
        } catch (IOException e) {
            System.out.println("Errore: impossibile rimuovere la rilevazione: " + e.getMessage());
            return;
        }
        try {
            AggregatorLink.Risposta r = link.invia(Protocol.REMOVED + " " + nome, null);
            if (r.ok()) {
                System.out.println("Rilevazione '" + nome + "' rimossa e notificata all'aggregatore.");
            } else {
                System.out.println("Rilevazione '" + nome + "' rimossa in locale, ma l'aggregatore ha risposto: "
                        + r.intestazione);
            }
        } catch (IOException e) {
            System.out.println("Rilevazione '" + nome + "' rimossa in locale, ma l'aggregatore non è raggiungibile.");
        }
    }

    /** download <nome> oppure download <peer> <nome> (estensione). */
    private void download(String[] t) {
        if (t.length == 2) {
            if (!Protocol.nomeValido(t[1])) {
                System.out.println("Errore: nome di rilevazione non valido.");
                return;
            }
            download.scarica(t[1]);
        } else if (t.length == 3) {
            if (!Protocol.peerIdValido(t[1]) || !Protocol.nomeValido(t[2])) {
                System.out.println("Uso: download <nome> | download <peer> <nome>");
                return;
            }
            download.scaricaDa(t[1], t[2]);
        } else {
            System.out.println("Uso: download <nome> | download <peer> <nome>");
        }
    }

    /** quit: comunica la disconnessione all'aggregatore, poi arresta il nodo. */
    private void esci() {
        try {
            link.invia(Protocol.DISCONNECT, null);
        } catch (IOException ignorata) {
            // aggregatore gia' irraggiungibile: il quit locale funziona comunque
        }
        link.chiudi();
        peerServer.chiudi();
        System.out.println("Nodo arrestato.");
        System.exit(0);
    }

    private void erroreAggregatore() {
        System.out.println("Errore: aggregatore non più raggiungibile.");
    }

    private void stampaHelp() {
        System.out.println("Comandi:");
        System.out.println("  listdata local                 rilevazioni possedute da questo nodo");
        System.out.println("  listdata remote [<nome>]       rilevazioni presenti sulla rete");
        System.out.println("  listdata peers                 altri nodi attivi");
        System.out.println("  add <nome> <contenuto>         aggiunge una rilevazione");
        System.out.println("  remove <nome>                  rimuove una rilevazione");
        System.out.println("  download <nome>                scarica dalla rete (nodo scelto dall'aggregatore)");
        System.out.println("  download <peer> <nome>         scarica da un nodo preciso");
        System.out.println("  quit                           arresta il nodo");
    }
}
