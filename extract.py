## Extract

import pandas as pd
import sqlalchemy

def print_user_comparison(user1, user2, user3):
    print("Course id overlap between users:")
    print("================================")
    print("User 1 and User 2 overlap: {}".format(
        set(user1.course_id) & set(user2.course_id)
    ))
    print("User 1 and User 3 overlap: {}".format(
        set(user1.course_id) & set(user3.course_id)
    ))
    print("User 2 and User 3 overlap: {}".format(
        set(user2.course_id) & set(user3.course_id)
    ))

# Complete the connection URI
connection_uri = "postgresql://repl:password@localhost:5432/datacamp_application"
db_engines = sqlalchemy.create_engine(connection_uri)

# Get user with id 4387
user1 = pd.read_sql("SELECT * FROM rating WHERE user_id=4387", db_engines)

# Get user with id 18163
user2 = pd.read_sql("SELECT * FROM rating WHERE user_id=18163", db_engines)

# Get user with id 8770
user3 = pd.read_sql("SELECT * FROM rating WHERE user_id=8770", db_engines)

# Use the helper function to compare the 3 users
print_user_comparison(user1, user2, user3)