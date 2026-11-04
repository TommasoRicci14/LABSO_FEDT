import java.io.File;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.util.List;

/**
 * Nodo sensore. Avvio:  java -cp bin Client <ip-aggregatore> <porta> [<cartella-dati>]
 *
 * La cartella dati (default "data/nodo") contiene le rilevazioni del nodo come file <nome>.txt.
 * Per avviare piu' nodi sulla stessa macchina basta indicare una cartella diversa per ciascuno.
 *
 * Thread: main (= ClientConsole), NodeListener, un PeerHandler per ogni FETCH in ingresso.
 */
public class Client {

    public static void main(String[] args) {
        if (args.length < 2 || args.length > 3) {
            System.err.println("Uso: java -cp bin Client <ip-aggregatore> <porta> [<cartella-dati>]");
            System.exit(1);
        }
        String ip = args[0];
        int porta = 0;
        try {
            porta = Integer.parseInt(args[1]);
            if (porta < 1 || porta > 65535) {
                throw new NumberFormatException();
            }
        } catch (NumberFormatException e) {
            System.err.println("Uso: java -cp bin Client <ip-aggregatore> <porta (1-65535)> [<cartella-dati>]");
            System.exit(1);
        }

        // 1) cartella dati: controllata PRIMA di connettersi, nessun file viene modificato
        String percorso = args.length == 3 ? args[2] : "data/nodo";
        File cartella = new File(percorso);
        if (!cartella.isDirectory() || !cartella.canRead() || !cartella.canWrite()) {
            System.err.println("Errore: cartella dati non accessibile: " + percorso);
            System.exit(1);
        }

        // 2) connessione all'aggregatore (nessun retry automatico)
        Socket socket = new Socket();
        try {
            socket.connect(new InetSocketAddress(ip, porta), 5_000);
        } catch (IOException | IllegalArgumentException e) {
            System.err.println("Errore: aggregatore non raggiungibile su " + ip + ":" + porta);
            System.exit(1);
        }

        LocalStore store = new LocalStore(cartella);
        NodeListener peerServer = null;
        AggregatorLink link = null;
        try {
            peerServer = new NodeListener(store);
            link = new AggregatorLink(socket);
        } catch (IOException e) {
            System.err.println("Errore: impossibile inizializzare il nodo: " + e.getMessage());
            System.exit(1);
        }
        peerServer.start();                        // gia' in ascolto prima della REGISTER

        // 3) REGISTER: l'host lo ricava l'aggregatore, la porta e' quella del NodeListener
        List<String> locali = store.elenco();
        String peerId = null;
        try {
            AggregatorLink.Risposta r = link.invia(Protocol.REGISTER + " " + peerServer.getPort()
                    + " " + locali.size(), locali.toArray(new String[0]));
            if (!r.ok()) {
                String codice = r.intestazione.startsWith("ERR ") ? r.intestazione.substring(4) : r.intestazione;
                System.err.println("Errore: registrazione rifiutata dall'aggregatore (" + codice + ").");
                peerServer.chiudi();
                link.chiudi();
                System.exit(1);
            }
            peerId = r.intestazione.substring(Protocol.OK.length()).trim();
        } catch (IOException e) {
            System.err.println("Errore: registrazione fallita: " + e.getMessage());
            peerServer.chiudi();
            link.chiudi();
            System.exit(1);
        }

        System.out.println("Connesso all'aggregatore " + ip + ":" + porta + " come " + peerId
                + " (porta di ascolto " + peerServer.getPort() + ", " + locali.size()
                + " rilevazioni). Digita 'help' per i comandi.");

        // 4) console interattiva sul thread main
        final AggregatorLink uplink = link;   // copia che non cambia più, usabile nella lambda
        RetrievalCoordinator download = new RetrievalCoordinator(peerId,
                richiesta -> uplink.invia(richiesta, null).intestazione,
                store);
        new ClientConsole(peerId, link, store, download, peerServer).esegui();
    }
}
