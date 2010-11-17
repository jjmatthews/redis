start_server {tags {"map"}} {
    proc create_map {key items} {
	    r del $key
	    foreach {score k entry} $items {
	        r tadd $key $score $k $entry
	    }
    }
    test {MAP basic TADD and value update} {
        r tadd mtmp 1 x
        r tadd mtmp 3 z
        r tadd mtmp 2 y
        set aux1 [r trange mtmp 0 -1 withscores]
        r tadd mtmp 1 xy
        set aux2 [r trange mtmp 0 -1 withscores]
        list $aux1 $aux2
    } {{1 x 2 y 3 z} {1 xy 2 y 3 z}}

    test {TLEN basics} {
        r tlen mtmp
    } {3}

    test {TLEN non existing key} {
        r tlen mtmp-blabla
    } {0}

}
