from dfp37 import *

db = Database(name="example_1")


@Column(name="uid", type_=int, unique=True, primary_key=True, autoincrement=True)
@Column(name="username", type_=str, unique=True)
@Column(name="password", type_=str)
class User(Table, database=db):
    pass


@Column(name="owner_id", type_=int)
@Column(name="name", type_=str)
@Column(name="description", type_=str)
class UserGroup(Table, database=db):
    pass


if not db.existed_before_runtime:
    print("build")
    db.build()

    User(username="admin", password="admin")
    User(username="azjlbzajkn", password="ecizavhzvaj")
    User(username="iadhzoiznakn", password="efoaljzn")

    db.commit()
else:
    print("already exists")

for user in User.findall():
    print(user)
