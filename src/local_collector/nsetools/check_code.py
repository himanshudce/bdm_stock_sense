from nsetools import Nse
nse = Nse()
q = nse.get_quote('HDFCBANK') # it's ok to use both upper or lower case for codes.
from pprint import pprint # just for neatness of display
pprint(q)
