import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;

/*
MECCANISMO 1: Tabella delle rilevazioni dell'aggregatore, in mutua esclusione TOTALE.

COSA CONTIENE
- possessori: per ogni risorsa (rilevazione), la lista dei peer che la possiedono.
- registro: per ogni peer, il suo indirizzo (host, porta) e se è ancora attivo.

PERCHÉ SERVE LA SINCRONIZZAZIONE
L'aggregatore ha un thread per ogni nodo connesso (NodeHandler). Tutti questi thread
leggono e modificano quetsa stessa tabella. Senza protezione, due thread che la toccano
insieme potrebbero lasciarla in uno stato corrotto (es. due REGISTER simultanee che
assegnano lo stesso peerId). Per evitarlo, tutti i metodi pubblici sono synchronized:
il "monitor" è l'istanza stessa, e solo UN thread alla volta può essere dentro uno
qualunque di questi metodi.

CLASSE "PASSIVA"
I suoi metodi toccano solo i propri dati e ritornano un risultato.
Non fanno mai chiamate di rete (I/O) e non chiamano altri oggetti sincronizzati.
Ogni metodo dura pochi microsecondi => nessun thread resta bloccato a lungo.
*/

public class AggregatorRegistry {

    /* Informazioni su un nodo registrato.
    peerId, host e porta sono "final" per evitare modifiche accidentali fuori dal monitor;
    attivo cambia solo con disconnetti(), sempre sotto monitor */
    public static class PeerInfo{
        public final String peerId;
        public final String host;
        public final int porta;
        boolean attivo; // diventa false quando il nodo si disconnette

        PeerInfo(String peerId, String host, int porta, boolean attivo){
            this.peerId = peerId;
            this.host = host;
            this.porta = porta;
            this.attivo = attivo;
        }

        public boolean isAttivo() {
            return attivo;
        }

        /*Restituisce una copia indipendente dei dati. In questo modo chi legge le informazioni 
        non può modificare per sbaglio l'oggetto originale custodito nella tabella*/
        PeerInfo copia() {
            return new PeerInfo(peerId, host, porta, attivo);
        }
    }

    /* Oggetto immutabile che rappresenta il risultato di una ricerca di download: 
    contiene peer, host e porta del nodo scelto.
    Se non c'è nessun candidato peer vale null (vedi nessuno()). */
    public static class EsitoRisolvi {
        public final String peer;
        public final String host;
        public final int porta;

        EsitoRisolvi(String peer, String host, int porta){
            this.peer = peer;
            this.host = host;
            this.porta = porta;
        }

        static EsitoRisolvi nessuno() {
            return new EsitoRisolvi(null, null, 0);
        }
    }

    /* Comparatore che ordina i peerId per suffisso numerico. 
    Serve per rispettare l'ordinamento esatto richiesto dalle specifiche per gli output.*/
    private static final Comparator<String> PER_SUFFISSO = new Comparator<String>() {
        @Override
        public int compare(String a, String b){
            return Integer.compare(suffisso(a), suffisso(b));
        }
    };

    // Estrae il numero finale da "peerX" --> se c'è un errore lo butta in fondo alla lista
    // ("peer" sono 4 caratteri, quindi substring(4) lascia solo il numero)
    private static int suffisso(String peerId) {
        try{
            return Integer.parseInt(peerId.substring(4));
        } catch (RuntimeException e){
            return Integer.MAX_VALUE;
        }
    }

    // Dati protetti dal monitor (accessibili solo tramite metodi synchronized). Sono private: nessuno fuori da questa classe può toccarli senza passare dal monitor.
    private final HashMap<String, ArrayList<String>> possessori = new HashMap<>(); //Indice dei file: dato il nome della risorsa, mi dice quali peer ce l'hanno
    private final HashMap<String, PeerInfo> registro = new HashMap<>(); // Anagrafica dei nodi: dal nome del peer ricavo subito IP, porta e se è attivo
    private int prossimoId = 0; // contatore per assegnare peer0, peer1, peer2, ...

    /*Registra il nodo e tutte le sue rilevazioni in un'unica operazione atomica.
      I nomi sono già stati letti dal socket (fuori dal monitor) -> un client lento non blocca gli altri.
      Gli id peerN non vengono mai riutilizzati (il contatore cresce soltanto)*/
    public synchronized String completaRegistrazione(String host, int porta, List<String> nomi){
        String peerId = "peer" + prossimoId;
        prossimoId++;
        registro.put(peerId, new PeerInfo(peerId, host, porta, true));
        for (String nome : nomi){
            aggiungiInterno(peerId, nome);
        }
        return peerId;
    }

    // Usato da ADDED e da RELEASE OK. Ritorna false se il peer non è registrato.
    public synchronized boolean aggiungiRisorsa( String peerId, String risorsa){
        if(!registro.containsKey(peerId)){
            return false;
        }

        aggiungiInterno(peerId, risorsa);
        return true;
    }

    // Usato da REMOVED. Ritorna false se il peer non possiede la risorsa.
    public synchronized boolean rimuoviRisorsa(String peerId, String risorsa){
        return rimuoviInterno(peerId, risorsa);
    }

    // Chiamato quando un nodo fa quit o cade. Non cancella le sue risorse dalla tabella
    public synchronized void disconnetti(String peerId) {
        PeerInfo info= registro.get(peerId);
        if (info != null){
            info.attivo= false; // Lo marco inattivo, la pulizia vera e propria la fa risolvi()
        }
    }

    /*Cerca chi ha il file. Mentre scorre la lista, ne approfitta per cancellare
    i nodi crashati o quelli a cui ci siamo già collegati senza successo (esclusi).
    Alla fine sceglie il peer con il numero più basso.
    
    Parametri:
    -richiedente = chi vuole scaricare (non può scaricare da se stesso)
    -risorsa = nome della rilevazione cercata
    -esclusi = peer già provati e falliti in questo download (Non deve essere null) */
    public synchronized EsitoRisolvi risolvi(String richiedente, String risorsa, Set<String> esclusi) {
        ArrayList<String> lista = possessori.get(risorsa);
        if (lista==null){
            return EsitoRisolvi.nessuno();
        }
        String migliore = null;
        // Si usa l'Iterator perché dentro il ciclo si rimuovono elementi dalla lista.
        Iterator<String> it = lista.iterator();
        while (it.hasNext()) {
            String p = it.next();
            PeerInfo info = registro.get(p);

            /* Rimuove il nodo dalla lista se è offline (crash/quit) 
             o se il client ha già provato a contattarlo senza successo (escluso).*/
            if(esclusi.contains(p) || info == null || !info.attivo) {
                it.remove();
                continue;
            }
            // Il richiedente viene solo saltato (non rimosso): possiede davvero la risorsa, semplicemente non può essere il proprio fornitore.
            if (p.equals(richiedente)){
                continue;
            }
            //Seleziona il nodo candidato che possiede l'identificativo numerico più basso
            if (migliore == null || PER_SUFFISSO.compare(p, migliore)<0){
                migliore = p;
            }
        }
        // Se la pulizia ha svuotato la lista, si toglie la risorsa dalla tabella (così non resta una riga vuota nel listdata).
        if (lista.isEmpty()) {
            possessori.remove(risorsa);
        }
        if (migliore == null) {
            return EsitoRisolvi.nessuno();
        }
        PeerInfo scelto = registro.get(migliore);
        return new EsitoRisolvi(scelto.peerId, scelto.host, scelto.porta);
    }

    /* Stessa logica di risolvi() ma senza eliminare nulla. 
       Verifica solo se il nodo richiesto ha davvero il file ed è online.*/    
    public synchronized EsitoRisolvi risolviMirato(String richiedente, String peerId, String risorsa) {
        if (peerId.equals(richiedente)) {
            return EsitoRisolvi.nessuno();
        }
        PeerInfo info= registro.get(peerId);
        ArrayList<String> lista= possessori.get(risorsa);
        if (info == null || !info.attivo || lista == null || !lista.contains(peerId)) {
            return EsitoRisolvi.nessuno();
        }
        return new EsitoRisolvi(info.peerId, info.host, info.porta);
    }

    /*Restituisce l'elenco delle risorse, una riga per risorsa - il nome seguito dai peer che la possiedono.
      Se nome è null restituisce tutte le risorse, altrimenti solo quella indicata.
      Le risorse sono in ordine alfabetico, i peer in ordine numerico. Include anche i peer disconnessi, perchè le loro risorse restano in tabella.
      Restituisce stringhe NUOVE (una copia): nessun riferimento ai dati interni esce dal monitor. */
    public synchronized List<String> elencoRisorse(String nome) {
        ArrayList<String> righe= new ArrayList<>();
        // TreeSet = insieme che tiene i nomi ordinati alfabeticamente
        TreeSet<String> nomi = new TreeSet<>(possessori.keySet());
        for (String risorsa : nomi){
            if ( nome != null && !nome.equals(risorsa)){
                continue; // è stato chiesto un nome preciso e questo non è lui: salta
            }
            // Copia della lista dei peer: si ordina la copia, non quella originale
            ArrayList<String> peers = new ArrayList<>(possessori.get(risorsa));
            Collections.sort(peers, PER_SUFFISSO);
            StringBuilder sb = new StringBuilder(risorsa);
            for (String p : peers) {
                sb.append(' ').append(p);
            }
            righe.add(sb.toString());
        }
        return righe;
    }

    // Solo i nodi attivi, ordinati per suffisso numerico.
    public synchronized List<String> elencoPeerAttivi() {
        ArrayList<String> attivi = new ArrayList<>();
        for (PeerInfo info : registro.values()) {
            if (info.attivo) {
                attivi.add(info.peerId);
            }
        }
        Collections.sort(attivi, PER_SUFFISSO);
        return attivi;
    }

    // Copia dei dati di un peer, o null se sconosciuto (la copia protegge i dati interni).
    public synchronized PeerInfo indirizzoDi(String peerId) {
        PeerInfo info = registro.get(peerId);
        return info == null ? null : info.copia();
    }

    //Metodi privati ausiliari. Evitano di duplicare il codice per l'inserimento e la rimozione nelle HashMap.
    //Non sono synchronized: li chiamano solo metodi che sono già dentro il monitor.
    private void aggiungiInterno(String peerId, String risorsa) {
        ArrayList<String> lista = possessori.get(risorsa);
        if (lista == null){
            lista = new ArrayList<>(); // prima volta che questa risorsa compare: crea la lista
            possessori.put(risorsa, lista);
        }
        if (!lista.contains(peerId)) { // evita duplicati dello stesso peer
            lista.add(peerId);
        }
    }

    private boolean rimuoviInterno(String peerId, String risorsa) {
        ArrayList<String> lista = possessori.get(risorsa);
        if( lista == null || !lista.remove(peerId)) {
            return false;
        }
        if (lista.isEmpty()) {
            possessori.remove(risorsa); // nessuno la possiede più: via anche la riga
        }
        return true;
    }

}