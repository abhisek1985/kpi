import pickle
import random
import base64

from Crypto.Cipher import AES
from random import randint

def random_with_N_digits(n):
    range_start = 10**(n-1)
    range_end = (10**n)-1
    return randint(range_start, range_end)


secret_key = str(random_with_N_digits(16))
cipher = AES.new(secret_key,AES.MODE_ECB)
encoded = base64.b64encode(cipher.encrypt("karakira".rjust(32)))

favorite_details = { "cipher": cipher, "encoded": encoded}
pickle.dump(favorite_details, open( "cipher.pkl", "wb" ))