from server.config import SessionLocal, engine
db = SessionLocal()
from server import models

models.Base.metadata.create_all(bind =engine)
db_record = models.Fake( cases = 'hell0')
print(db_record)
db.add(db_record)
db.commit()
db.close()