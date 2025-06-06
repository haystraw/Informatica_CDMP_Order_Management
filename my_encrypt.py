from cryptography.fernet import Fernet
import getpass

version = 20230420

def generate_key():
    """
    Generates a key and save it into a file
    """
    key = Fernet.generate_key()
    with open("secret.key", "wb") as key_file:
        key_file.write(key)

def load_key():
    """
    Load the previously generated key
    """
    return open("secret.key", "rb").read()

def encrypt_message(message):
    """
    Encrypts a message
    """
    key = load_key()
    encoded_message = message.encode()
    f = Fernet(key)
    encrypted_message = f.encrypt(encoded_message)

    return encrypted_message.decode()

def decrypt_message(encrypted_message):
    """
    Decrypts an encrypted message
    """
    message = encrypted_message.encode()
    key = load_key()
    f = Fernet(key)
    decrypted_message = f.decrypt(message)

    return decrypted_message.decode()

def getEncryptedString():
    string = getpass.getpass('Enter Text to encrypt:')
    encrypted = encrypt_message(string)
    print(f"Encrypted String:{encrypted}")
    '''
    decrypted = decrypt_message(encrypted)
    print(f"Decrypted: {decrypted}")
    '''
def verifyEncryption():
    string = input("Verify Encrypted String:")
    decrypted = decrypt_message(string)
    print(f"Decrypted Value: {decrypted}")
    
if __name__ == "__main__":
    regen = input("Want to generate a new secret.key? This will invalidate any previous encrypted passwords (Y/N): ")
    if regen.upper() == 'Y':
        print(f"Generating new secret.key file")
        generate_key()
    else:
        getEncryptedString()
        verifyEncryption()

