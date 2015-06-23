#!/usr/bin/env python
from __future__ import absolute_import, division, print_function, unicode_literals
import re


def _decode_list(mv):
    return [match[0].replace('$$', '$') for match in encoded_value.findall(mv)]

encoded_value = re.compile(r'\$(?P<item>(?:\$\$|[^$])*)\$(;|$)')  # matches a single value in an encoded list
print(_decode_list("$1$;$2aldj$$faldsj$$$;$foo$$bar$$$$1$;$2aldj$$faldsj$$$;$foo$$bar$$$$1$;$2aldj$$faldsj$$$;$foo$$bar$$$$1$;$2aldj$$faldsj$$$;$foo$$bar$$$"))