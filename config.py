class Config:
    DEBUG = True
    SECRET_KEY = 'your-secret-key-here'  # Change this in production!
    DATABASE_URI = 'mysql://user:userpassword@localhost:3306/mydb' 

# Database connection settings
DW_CONFIG = {
    'host': 'mysql_db',
    'user': 'user',
    'password': 'userpassword',
    'database': 'DW_Varejo'
} 