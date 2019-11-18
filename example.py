from pyorm import connect_db, connecting
from pyorm import Model, Column, Integer, Varchar

class Student(Model):
    __table_name__ = "student"

    ID = Column(Integer(), auto_increment=True)
    name = Column(Varchar(), nullable=False)
    age = Column(Integer(), nullable=True, default=0)

with connecting('sqlite', 'tmp.db') as db:
    db.logging(True)
    db.create_all()
    stu1 = Student(name="Shubham", age=23)
    db.insert(stu1)
    db.commit()
    print(db.query(Student).filter(Student.ID < 3).all())
