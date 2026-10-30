/**
 * MECCANISMO 2 - un nodo sensore serve UNA richiesta per volta; le altre ATTENDONO (mai errore).
 *
 * Monitor: l'istanza stessa (una per nodo sensore). Si tiene il FLAG, non il monitor: il
 * trasferimento vero e proprio avviene fuori da qualunque blocco synchronized.
 *
 * Il permesso e' preso dal lato che SERVE la richiesta: un nodo che sta scaricando continua a
 * servire le FETCH in ingresso. Se ogni nodo tenesse il flag anche durante un download in
 * uscita, due nodi che si scaricano a vicenda si bloccherebbero (deadlock distribuito).
 *
 * Regole: sempre while (condizione) wait() e mai if; sempre notifyAll() e mai notify().
 * Limite dichiarato: nessuna garanzia di ordine FIFO fra i thread in attesa.
 */
public class FetchGate {

    private boolean occupato = false;
    private String titolare = null;
    private int inAttesa = 0;           // solo diagnostica

    /**
     * Acquisisce il permesso, attendendo se e' occupato. Dichiara InterruptedException e non
     * la cattura: se arriva durante la wait() il permesso NON e' stato acquisito, quindi il
     * chiamante non deve chiamare rilascia().
     */
    public synchronized void acquisisci(String chi) throws InterruptedException {
        inAttesa++;
        try {
            while (occupato) {          // sempre while, mai if
                wait();
            }
        } finally {
            inAttesa--;                 // decrementato anche se arriva un'InterruptedException
        }
        occupato = true;
        titolare = chi;
    }

    /** Rilascia il permesso e risveglia i thread in attesa. */
    public synchronized void rilascia() {
        occupato = false;
        titolare = null;
        notifyAll();                    // sempre notifyAll, mai notify
    }

    // ---- diagnostica (usata solo per le stampe) ----

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