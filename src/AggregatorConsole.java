import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.List;

/**
 * Thread della console dell'aggregatore: legge dallo standard input (stdin) 
 * e gestisce i comandi interattivi (listdata, log, quit, help).
 * Nota: l'uso di BufferedReader è consentito esclusivamente qui su System.in 
 * e non deve essere utilizzato sui socket.
 */
public class AggregatorConsole extends Thread {

    private final Aggregator aggregatore;
    private final AggregatorRegistry stato;
    private final DownloadLog log;

    public AggregatorConsole(Aggregator aggregatore, AggregatorRegistry stato, DownloadLog log) {
        super("AggregatorConsole");
        this.aggregatore = aggregatore;
        this.stato = stato;
        this.log = log;
    }

    @Override
    public void run() {
        BufferedReader tastiera = new BufferedReader(new InputStreamReader(System.in));
        try {
            while (true) {
                System.out.print("> ");
                System.out.flush();
                String riga = tastiera.readLine();
                if (riga == null) {
                    aggregatore.arresta();                  // EOF (Ctrl-D) = quit
                    return;
                }
                riga = riga.trim();
                if (riga.isEmpty()) {
                    continue;
                }
                if (riga.equals("listdata")) {
                    List<String> righe = stato.elencoRisorse(null);
                    System.out.println("Risorse:");
                    for (String r : righe) {
                        System.out.println("- " + Protocol.formattaRiga(r));
                    }
                } else if (riga.equals("log")) {
                    System.out.println("Risorse scaricate:");
                    for (String r : log.righe()) {
                        System.out.println(r);
                    }
                } else if (riga.equals("quit")) {
                    aggregatore.arresta();
                    return;
                } else if (riga.equals("help")) {
                    System.out.println("Comandi: listdata | log | quit | help");
                } else {
                    System.out.println("Comando non riconosciuto. Digita 'help'.");
                }
            }
        } catch (IOException e) {
            aggregatore.arresta();
        }
    }
}
