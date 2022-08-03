import hashlib


def hash512(text: str) -> str:
    """
    >>> hash512('| makeresults count=1| eval metric="fact1", value=50, order=1| append [ | makeresults count=1 | eval metric="plan1", value=85, order=2]| append [ | makeresults count=1 | eval metric="diff1", value=-35, order=3]| append [ | makeresults count=1 | eval metric="fact2", value=15, order=4]| append [ | makeresults count=1 | eval metric="plan2", value=50, order=5]| append [ | makeresults count=1 | eval metric="diff2", value=-35, order=6]| append [ | makeresults count=1 | eval metric="_title", value="42" ]')
    '59664f19132179f8587fd873d428c3edee7cedf3b30aa190bc6e23155060fe537c12b610d30cc6ee53956cbe2ba7521925fcf56caa0f541de6705aeb222b77ce'
    """
    text = str(hashlib.sha512(text.encode('utf-8')).hexdigest())
    return text
