start_server {tags {"ts"}} {
    proc create_map {key items} {
	    r del $key
	    foreach {score k entry} $items {
	        r tadd $key $score $k $entry
	    }
    }
    test {TS basic TADD and value update} {
        r tadd ttmp 1 x
        r tadd ttmp 3 z
        r tadd ttmp 2 y
        set aux1 [r trange ttmp 0 -1 withscores]
        r tadd ttmp 1 xy
        set aux2 [r trange ttmp 0 -1 withscores]
        list $aux1 $aux2
    } {{1 x 2 y 3 z} {1 xy 2 y 3 z}}

    test {TLEN basics} {
        r tlen ttmp
    } {3}

    test {TLEN non existing key} {
        r tlen ttmp-blabla
    } {0}

    test "TRANGE basics" {
        r del ttmp
        r tadd ttmp 1 a
        r tadd ttmp 2 b
        r tadd ttmp 3 a
        r tadd ttmp 4 c

        assert_equal {a b a c} [r trange ttmp 0 -1]
        assert_equal {a b a} [r trange ttmp 0 -2]
        assert_equal {b a c} [r trange ttmp 1 -1]
        assert_equal {b a} [r trange ttmp 1 -2]
        assert_equal {a c} [r trange ttmp -2 -1]
        assert_equal {a} [r trange ttmp -2 -2]

        # out of range start index
        assert_equal {a b a} [r trange ttmp -5 2]
        assert_equal {a b} [r trange ttmp -5 1]
        assert_equal {} [r trange ttmp 5 -1]
        assert_equal {} [r trange ttmp 5 -2]

        # out of range end index
        assert_equal {a b a c} [r trange ttmp 0 5]
        assert_equal {b a c} [r trange ttmp 1 5]
        assert_equal {} [r trange ttmp 0 -5]
        assert_equal {} [r trange ttmp 1 -5]

        # withscores
        assert_equal {1 a 2 b 3 a 4 c} [r trange ttmp 0 -1 withscores]
    }

}
