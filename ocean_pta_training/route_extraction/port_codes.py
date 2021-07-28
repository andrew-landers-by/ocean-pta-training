import string


def lower_case_greek_letters() -> str:
    return ''.join(map(
            chr, range(ord('\N{GREEK SMALL LETTER ALPHA}'),
                       ord('\N{GREEK SMALL LETTER OMEGA}')+1)
    ))

def upper_case_greek_letters() -> str:
    return ''.join(map(
        chr, range(ord('\N{GREEK CAPITAL LETTER ALPHA}'),
                   ord('\N{GREEK CAPITAL LETTER OMEGA}') + 1)
    ))


PORT_LETTER_CHARS = (
    string.ascii_letters
    + string.digits
    + lower_case_greek_letters()
    + upper_case_greek_letters()
)

JOURNEY_BREAKER_LETTER = "\N{CROSS MARK}"
