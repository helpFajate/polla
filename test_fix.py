import bcrypt

password = b"123456"  # la contraseña que quieras
hash = bcrypt.hashpw(password, bcrypt.gensalt())

print(hash.decode())