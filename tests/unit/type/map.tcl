start_server {tags {"map"}} {
    proc create_map {key items} {
	    r del $key
	    foreach {score k entry} $items {
	        r tadd $key $score $k $entry
	    }
    }
    test {MAP basic TADD and value update} {
        r tadd mtmp 1 20010101 x
        r tadd mtmp 2 20040101 y
        r tadd mtmp 3 20070101 z
        set aux1 [r titems mtmp]
        r tadd mtmp 1 20010101 xy
        set aux2 [r titems mtmp]
        list $aux1 $aux2
    } {{20010101 x 20040101 y 20070101 z} {20010101 xy 20040101 y 20070101 z}}

    test {TLEN basics} {
        r tlen mtmp
    } {3}

    test {TLEN non existing key} {
        r tlen mtmp-blabla
    } {0}

}
