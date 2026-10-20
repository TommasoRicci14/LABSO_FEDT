import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

public class PeerLockManager{

    public static class Sessione {
        public final String token;
        public final String richiedente;
        public final String sorgente;
        public final String risorsa;
        public final long aperturaMs;

        Sessione(String token, String richiedente, String sorgente, String risorsa) {
            this.token = token;
            this.richiedente = richiedente;
            this.sorgente = sorgente;
            this.risorsa = risorsa;
            this.aperturaMs = System.currentTimeMillis();
        }
    }

    private final HashMap<String, String> titolarePerPeer = new HashMap<>();
    private final HashMap<String, Sessione> sessioni = new HashMap<>();
    private long prossimoToken = 0;

    public synchronized String acquisisci(String peerSorgente, String peerRichiedente, String risorsa) throws InterruptedException {
        while (titolarePerPeer.containsKey(peerSorgente)) {
            wait();
        }
        titolarePerPeer.put(peerSorgente, peerRichiedente) ;
        String token = "t" + prossimoToken;
        prossimoToken++;
        sessioni.put(token, new Sessione(token, peerRichiedente, peerSorgente, risorsa));
        return token;
    }

    public synchronized Sessione rilascia(String token) {
        Sessione s = sessioni.remove(token);
        if (s == null) {
            return null;
        }
        titolarePerPeer.remove(s.sorgente);
        notifyAll();
        return s;
    }

    public synchronized List<Sessione> rilasciaTuttiDi(String peerId) {
        ArrayList<Sessione> chiuse = new ArrayList<>();
        for (Sessione s : new ArrayList<>(sessioni.values())) {
            if (s.richiedente.equals(peerId) || s.sorgente.equals(peerId)) {
                sessioni.remove(s.token);
                titolarePerPeer.remove(s.sorgente);
                chiuse.add(s);
            }
        }
        titolarePerPeer.remove(peerId);
        notifyAll();
        return chiuse;
    }

    public synchronized Sessione leggi(String token) {
        return sessioni.get(token);
    }

}



