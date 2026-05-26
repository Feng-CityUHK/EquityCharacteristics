# set up the .pgpass file
# then you don't need to type in password
import wrds
db = wrds.Connection(wrds_username='xinhe97')
# Enter your WRDS username [joe]:
# Enter your password:
db.create_pgpass_file()
db.close()
# check again
db = wrds.Connection(wrds_username='xinhe97')
db.close()
