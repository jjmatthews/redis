
=============================
Extra Commands
=============================


ZDIFFSTORE
===================

Computes the difference between the first sorted set and all the successive sorted sets
given by the specified keys, and stores the result in destination.
It is mandatory to provide the number of input keys (numkeys)
before passing the input keys and the other (optional) arguments::

    ZDIFFSTORE destination numkeys key [key ...] withscore
    

If ``withscore`` is ``True`` elements are removed only if the score is matched.
