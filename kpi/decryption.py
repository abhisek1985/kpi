"""
This python file is used for encryption and decryption
over database password.
"""
from Crypto.Cipher import AES
from random import randint
import base64

def random_with_N_digits(n):
    range_start = 10**(n-1)
    range_end = (10**n)-1
    return randint(range_start, range_end)

def Encrypt(self):
    secret_key = base64.b64encode(str(random_with_N_digits(16)))
    cipher = AES.new(secret_key, AES.MODE_ECB)
    encoded = base64.b64encode(cipher.encrypt("karakira".rjust(32)))