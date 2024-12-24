from Crypto.Cipher import PKCS1_OAEP
from Crypto.PublicKey import RSA
import base64

with open ("demo-keys/public.pem", "rb") as pub_file:
    publicKey=pub_file.read()
key = RSA.importKey(publicKey)

payload=b"GB82EVJA51473322705367"
cipher = PKCS1_OAEP.new(key)
ciphertext = cipher.encrypt(payload)

# with open ("demo-keys/key.pem", "rb") as prv_file:
#     privateKey=prv_file.read()
# cipher = PKCS1_OAEP.new(key = RSA.importKey(privateKey))

# print(cipher.decrypt(ciphertext))
print(base64.b64encode(ciphertext))

