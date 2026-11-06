import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

/*MECCANISMO 3 del progetto: il "token" come LOCK PER NODO DESTINATARIO.
 Idea: per ogni peer che può fare da SORGENTE di un download esiste un solo "permesso" (il token). 
 Chi vuole scaricare da peerX deve prima ottenere il token di peerX; se un altro nodo lo sta già usando, 
 il richiedente ATTENDE (non riceve un errore) e viene servito al rilascio.
 Token di peer diversi sono indipendenti: un download da peer0 e uno da peer3 possono procedere in parallelo.
 
 Sincronizzazione: SOLO synchronized + wait()/notifyAll(). Il monitor è l'istanza stessa di questa classe.
 Perché è una classe SEPARATA da AggregatorState: chi aspetta un token resta fermo dentro wait() anche a lungo. 
 Se questa logica stesse nello stesso monitor della tabella delle risorse, un nodo in attesa bloccherebbe tutta la rete 
 (nessuno riuscirebbe nemmeno a fare una listdata). 
 Il NodeHandler quindi chiama questa classe FUORI da qualunque altro monitor.
 */

public class PeerLockManager{

    /* Una sessione = un tentativo di download (un token acquisito).
    Serve a due cose: sapere chi ha in mano quale token (per rilasciarlo alla disconnessione) e alimentare il log dell'aggregatore.
    È IMMUTABILE (tutti i campi final): per questo leggi() e rilascia() possono restituirla a chi chiama, fuori dal monitor, 
    senza rischi di accessi concorrenti sui suoi campi.
    */
    public static class Sessione {
        public final String token; // identificativo, forma "t0", "t1",...
        public final String richiedente; // peer che scarica (titolare del token)
        public final String sorgente; // peer da cui si scarica (quello "bloccato")
        public final String risorsa; // nome della rilevazione richiesta
        public final long aperturaMs; // istante di apertura, per il log

        // Costruttore senza public: le sessioni le crea solo acquisisci().
        Sessione(String token, String richiedente, String sorgente, String risorsa) {
            this.token = token;
            this.richiedente = richiedente;
            this.sorgente = sorgente;
            this.risorsa = risorsa;
            this.aperturaMs = System.currentTimeMillis();
        }
    }

    // peer SORGENTE -> peer TITOLARE del suo token.
    // Chiave presente = token occupato; chiave assente = token libero.
    // Questa mappa È il lock: la condizione di attesa è containsKey(sorgente).
    private final HashMap<String, String> titolarePerPeer = new HashMap<>();

    // token (es. "t3") -> Sessione. Permette di risalire dal token ricevuto nella RELEASE alla sorgente da liberare.
    private final HashMap<String, Sessione> sessioni = new HashMap<>();

    // Contatore per generare token univoci. 
    // Protetto dal monitor, quindi l'incremento (read-modify-write) non ha bisogno di altro.
    private long prossimoToken = 0;

    /* Acquisisce il token del peer sorgente per conto del richiedente. BLOCCA finché il token non è libero. 
    Ritorna l'id del nuovo token.
    Va chiamato solo DOPO aver scelto un candidato (un token non viene mai acquisito senza che il client riceva la risposta OK).
    Se arriva un'interruzione mentre si è in wait() il token NON è stato acquisito: l'eccezione esce al chiamante e non c'è nulla da rilasciare
    ("si rilascia solo ciò che si è acquisito"). */
    public synchronized String acquisisci(String peerSorgente, String peerRichiedente, String risorsa) throws InterruptedException {
        // while e NON if: dopo il risveglio bisogna ricontrollare la condizione, perché tra la notifyAll() 
        // e la riacquisizione del monitor un altro thread potrebbe essersi già preso il token (o il risveglio può essere
        // dovuto a un token di un altro peer, vedi notifyAll sotto).
        while (titolarePerPeer.containsKey(peerSorgente)) {
            wait(); // rilascia il monitor e dorme; lo riprende prima di ripartire
        }
        // Da qui il token è libero e siamo dentro il monitor: prenderlo e registrare la sessione avvengono in modo atomico.
        titolarePerPeer.put(peerSorgente, peerRichiedente) ;
        String token = "t" + prossimoToken;
        prossimoToken++;
        sessioni.put(token, new Sessione(token, peerRichiedente, peerSorgente, risorsa));
        return token;
    }

    /* Rilascia il token (a fine download, riuscito o fallito) e sveglia chi era in attesa. 
    Ritorna la sessione chiusa, oppure null se il token non esiste o è già stato chiuso (in quel caso non fa nulla).*/
    public synchronized Sessione rilascia(String token) {
        Sessione s = sessioni.remove(token);
        if (s == null) {
            return null; // niente da fare, e niente notifyAll: nulla è cambiato
        }
        titolarePerPeer.remove(s.sorgente); // il token della sorgente torna libero

        // notifyAll e NON notify: nel monitor attendono thread che aspettano token di peer DIVERSI. 
        // notify() ne sveglierebbe uno a caso, magari uno che aspetta un altro peer (tornerebbe a dormire) lasciando
        // addormentato per sempre proprio quello che poteva procedere.
        // Costo: risvegli inutili, accettabili con pochi nodi.
        notifyAll();
        return s;
    }

    /* Rilascio automatico alla disconnessione di un nodo (crash o quit).
     È il caso limite più importante del meccanismo: senza questo, un client morto con un token in mano renderebbe il nodo sorgente
     inaccessibile per sempre, e chi lo aspetta dormirebbe a tempo indeterminato.
     
     Chiude due categorie di sessioni:
     (a) quelle in cui peerId era RICHIEDENTE (teneva il token di qualcun altro);
     (b) quelle in cui peerId era SORGENTE (qualcuno teneva il suo token).
     Ritorna le sessioni chiuse, che il chiamante scrive nel log come FALLITO.
     
     Il NodeHandler deve chiamarla su OGNI percorso di uscita (in un finally), non solo quando readLine ritorna null: 
     se il client muore mentre il suo handler è in wait(), non c'è nessun EOF da vedere. */
    public synchronized List<Sessione> rilasciaTuttiDi(String peerId) {
        ArrayList<Sessione> chiuse = new ArrayList<>();
        // Si itera su una COPIA dei valori perché dentro il ciclo si rimuove da 'sessioni': 
        // modificare la mappa mentre la si scorre darebbe ConcurrentModificationException.
        for (Sessione s : new ArrayList<>(sessioni.values())) {
            if (s.richiedente.equals(peerId) || s.sorgente.equals(peerId)) {
                sessioni.remove(s.token);
                titolarePerPeer.remove(s.sorgente); // libera il token della sorgente di quella sessione
                chiuse.add(s);
            }
        }
        
        titolarePerPeer.remove(peerId);
        notifyAll(); // chi aspettava uno dei token appena liberati può ripartire
        return chiuse;
    }

    /*Sola lettura di una sessione (usata per il log). Non modifica nulla, quindi non serve notifyAll. 
    È synchronized comunque, perché HashMap non è thread-safe e un get() concorrente a un remove() è un data race.*/
    public synchronized Sessione leggi(String token) {
        return sessioni.get(token);
    }

}



