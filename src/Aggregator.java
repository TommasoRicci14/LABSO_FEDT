    import java.io.IOException;
    import java.net.ServerSocket;
    import java.net.Socket;
    import java.util.ArrayList;

    /**
     * Nodo aggregatore principale del sistema distribuito ---> cosa intendi per sistema distribuito?
     *
     * Si occupa di inizializzare il server sulla porta indicata, gestire il ciclo 
     * di accettazione delle connessioni (accept loop) in ambiente multithreading 
     * e coordinare la chiusura ordinata dell'intera applicazione.
     * 
     * Gestisce inoltre i riferimenti agli oggetti condivisi protetti da monitor:
     * - AggregatorState (Meccanismo 1 - Stato e lock centralizzati)
     * - TokenManager (Meccanismo 3 - Gestione token di accesso)
     * - DownloadLog (Registro storico delle operazioni di download)
     * 
     * Avvio da riga di comando: java -cp bin Aggregator <porta>
     */
    public class Aggregator {

        private final ServerSocket serverSocket;
        private final AggregatorState stato = new AggregatorState();
        private final TokenManager token = new TokenManager();
        private final DownloadLog log = new DownloadLog();

        // Lista dei thread NodeHandler vivi: protetta da synchronized (handlers) corti.
        private final ArrayList<NodeHandler> handlers = new ArrayList<>();
        private volatile boolean inChiusura = false;

        public Aggregator(ServerSocket serverSocket) {
            this.serverSocket = serverSocket;
        }

        public static void main(String[] args) {
            if (args.length != 1) {
                usoErrato();
            }
            int porta = 0;
            try {
                porta = Integer.parseInt(args[0]);
            } catch (NumberFormatException e) {
                usoErrato();
            }
            if (porta < 1 || porta > 65535) {
                usoErrato();
            }
            ServerSocket ss = null;
            try {
                ss = new ServerSocket(porta);
            } catch (IOException e) {
                System.err.println("Errore: impossibile mettersi in ascolto sulla porta " + porta
                        + ": " + e.getMessage());
                System.exit(1);
            }
            new Aggregator(ss).esegui();
        }

        private static void usoErrato() {
            System.err.println("Uso: java -cp bin Aggregator <porta (1-65535)>");
            System.exit(1);
        }

        /** Avvia la console e ripete accept() creando un NodeHandler per connessione. */
        public void esegui() {
            System.out.println("Aggregatore in ascolto sulla porta " + serverSocket.getLocalPort()
                    + ". Digita 'help' per i comandi.");
            new AggregatorConsole(this, stato, log).start();

            while (!inChiusura) {
                try {
                    Socket socket = serverSocket.accept();
                    NodeHandler handler = new NodeHandler(socket, stato, token, log, handlers);
                    synchronized (handlers) {
                        handlers.add(handler);
                    }
                    handler.start();
                } catch (IOException e) {
                    if (inChiusura || serverSocket.isClosed()) {
                        break;
                    }
                    System.err.println("Errore in accept: " + e.getMessage());
                }
            }
        }

        /**
         * Chiusura ordinata (comando quit): chiude il ServerSocket, copia la lista degli handler
         * FUORI dal blocco synchronized e chiude i loro socket (sveglia anche chi attende un token).
         */
        public void arresta() {
            inChiusura = true;
            try {
                serverSocket.close();
            } catch (IOException ignorata) {
                // niente da fare
            }
            ArrayList<NodeHandler> copia;
            synchronized (handlers) {
                copia = new ArrayList<>(handlers);
            }
            for (NodeHandler h : copia) {
                h.chiudi();
            }
            System.out.println("Aggregatore arrestato.");
            System.exit(0);
        }
    }
