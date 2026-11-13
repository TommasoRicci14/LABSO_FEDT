# LABSO_FEDT

Compilazione:
    mkdir -p bin
    javac -d bin src/*.java

Avvio (un terminale per ognuno):
    java -cp bin Aggregator 9000
    java -cp bin Client 127.0.0.1 9000 data/nodeA
    java -cp bin Client 127.0.0.1 9000 data/nodeB
    java -cp bin Client 127.0.0.1 9000 data/nodeC

Comandi nodo: listdata local | listdata remote | add <nome> <contenuto> | download <nome> | quit
Comandi aggregatore: listdata | log | quit