import re

REGEX_CONSEC_DUPLICATE_CHARS = re.compile(r'(.)\1+')

def remove_consec_dup_chars(s: str):
    return re.sub(REGEX_CONSEC_DUPLICATE_CHARS, r'\1', s)


def str_has_dup_chars(s: str):
    return len(s) != len(set(s))

def intermed_port_chars_admissible(s: str):
    return not str_has_dup_chars(remove_consec_dup_chars(s))

