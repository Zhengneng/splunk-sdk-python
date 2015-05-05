#!/usr/bin/env python


def _decode_list(mv):
    if len(mv) == 0:
        return None
    in_value = False
    value = ''
    i = 0
    l = []
    while i < len(mv):
        if not in_value:
            if mv[i] == '$':
                in_value = True
            elif mv[i] != ';':
                return None
        else:
            if mv[i] == '$' and i + 1 < len(mv) and mv[i + 1] == '$':
                value += '$'
                i += 1
            elif mv[i] == '$':
                in_value = False
                l.append(value)
                value = ''
            else:
                value += mv[i]
        i += 1
    return l

print(_decode_list("$1$;$2aldj$$faldsj$$$;$foo$$bar$$$$1$;$2aldj$$faldsj$$$;$foo$$bar$$$$1$;$2aldj$$faldsj$$$;$foo$$bar$$$$1$;$2aldj$$faldsj$$$;$foo$$bar$$$"))