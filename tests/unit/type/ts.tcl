start_server {tags {"ts"}} {
    proc create_map {key items} {
	    r del $key
	    foreach {score k entry} $items {
	        r tadd $key $score $k $entry
	    }
    }
    test {TS basic TSADD and value update} {
        r tsadd ttmp 1 x
        r tsadd ttmp 3 z
        r tsadd ttmp 2 y
        set aux1 [r tsrange ttmp 0 -1 withtimes]
        r tsadd ttmp 1 xy
        set aux2 [r tsrange ttmp 0 -1 withtimes]
        list $aux1 $aux2
    } {{1 x 2 y 3 z} {1 xy 2 y 3 z}}

    test {TSLEN basics} {
        r tslen ttmp
    } {3}

    test {TSLEN non existing key} {
        r tslen ttmp-blabla
    } {0}

    test "TSRANGE basics" {
        r del ttmp
        r tsadd ttmp 1 a
        r tsadd ttmp 2 b
        r tsadd ttmp 3 a
        r tsadd ttmp 4 c

        assert_equal {a b a c} [r tsrange ttmp 0 -1]
        assert_equal {a b a} [r tsrange ttmp 0 -2]
        assert_equal {b a c} [r tsrange ttmp 1 -1]
        assert_equal {b a} [r tsrange ttmp 1 -2]
        assert_equal {a c} [r tsrange ttmp -2 -1]
        assert_equal {a} [r tsrange ttmp -2 -2]

        # out of range start index
        assert_equal {a b a} [r tsrange ttmp -5 2]
        assert_equal {a b} [r tsrange ttmp -5 1]
        assert_equal {} [r tsrange ttmp 5 -1]
        assert_equal {} [r tsrange ttmp 5 -2]

        # out of range end index
        assert_equal {a b a c} [r tsrange ttmp 0 5]
        assert_equal {b a c} [r tsrange ttmp 1 5]
        assert_equal {} [r tsrange ttmp 0 -5]
        assert_equal {} [r tsrange ttmp 1 -5]

        # withscores
        assert_equal {1 a 2 b 3 a 4 c} [r tsrange ttmp 0 -1 withtimes]
    }

    test "TSRANGEBYTIME basics" {
        r del ttmp
        r tsadd ttmp 45 achtung
        r tsadd ttmp 58 goodbye
        r tsadd ttmp 104 kaput
        r tsadd ttmp 23 ciao
        r tsadd ttmp 80 ciao

        assert_equal {} [r tsrangebytime ttmp 1 15]
        assert_equal {ciao achtung} [r tsrangebytime ttmp 20 50]
        assert_equal {ciao achtung goodbye ciao} [r tsrangebytime ttmp 20 80]

        # out of range start index
        assert_equal {} [r tsrangebytime ttmp 0 1]

        # with times
        assert_equal {23 ciao 45 achtung 58 goodbye} [r tsrangebytime ttmp 20 60 withtimes]
    }
}
