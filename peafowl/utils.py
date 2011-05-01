# -*- coding: utf-8 -*-


def rusage_user():
    try:
        from resource import getrusage, RUSAGE_SELF
        return getrusage(RUSAGE_SELF)[0]
    except:
        return 0


def rusage_system():
    try:
        from resource import getrusage, RUSAGE_SELF
        return getrusage(RUSAGE_SELF)[1]
    except:
        return 0

