/**
 * Gestisce il lock locale del nodo (Meccanismo 2): serve una richiesta FETCH alla volta
 * e mette in attesa le altre con wait(), senza mai restituire errore.
 * 
 * Teniamo occupato solo il flag e non il monitor durante l'invio dei byte, così il 
 * trasferimento vero e proprio avviene fuori dal blocco synchronized.
 * Inoltre il lock viene preso solo quando il nodo fa da server: se lo bloccassimo anche 
 * mentre scarica da altri, due nodi che scaricano a vicenda andrebbero in deadlock.
 */
public class FetchGate {

    private boolean occupato = false;
    private String titolare = null;
    private int inAttesa = 0;           // contatore per le stampe di log

    /**
     * Prende il lock per servire un peer, o si mette in attesa se il nodo è già occupato.
     * Lascia propagare InterruptedException: se salta durante la wait() non abbiamo 
     * preso il lock, quindi nel PeerHandler non andrà chiamato rilascia().
     */
    public synchronized void acquisisci(String chi) throws InterruptedException {
        inAttesa++;
        try {
            while (occupato) {          // while per evitare i risvegli spuri
                wait();
            }
        } finally {
            inAttesa--;                 // lo scala sempre, anche se viene interrotto
        }
        occupato = true;
        titolare = chi;
    }

    /** Libera il nodo a fine trasferimento e sveglia gli altri thread in coda. */
    public synchronized void rilascia() {
        occupato = false;
        titolare = null;
        notifyAll();
    }

    // Getter usati per le stampe a schermo

    public synchronized boolean isOccupato() {
        return occupato;
    }

    public synchronized String getTitolare() {
        return titolare;
    }

    public synchronized int getInAttesa() {
        return inAttesa;
    }
}