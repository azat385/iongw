from sqlalchemy import create_engine
from sqlalchemy import Column, Integer, String, ForeignKey, REAL
from sqlalchemy.orm import relationship
from sqlalchemy.ext.declarative import declarative_base

db_name = "common.db"

Base = declarative_base()

class Device(Base):
    __tablename__ = 'device'
    id = Column(Integer, primary_key=True)
    slave_num = Column(Integer)
    name = Column(String(50), nullable=False, unique=True)
    desc_short = Column(String(50))
    desc_long = Column(String(250))

    def __repr__(self):
        return "<Device(id='{}', name='{}')>".format(self.id, self.name)


class Tag(Base):
    __tablename__ = 'tag'
    id = Column(Integer, primary_key=True)
    order_num = Column(Integer)
    name = Column(String(50), nullable=False, unique=True)
    desc = Column(String(500))

    def __repr__(self):
        return "<Tag(id='{}', name='{}')>".format(self.id, self.name)


class Data(Base):
    __tablename__ = 'data'
    id = Column(Integer, primary_key=True)
    device_id = Column(Integer, ForeignKey('device.id'))
    device = relationship(Device)
    tag_id = Column(Integer, ForeignKey('tag.id'))
    tag = relationship(Tag)
    value = Column(REAL)
    stime = Column(String(50))

    def __repr__(self):
        return "<Data(id='{}', device='{}' tag='{}' )>".format(
            self.id, self.device_id, self.tag_id, )


engine = create_engine('sqlite:///{}'.format(db_name), echo=True)

Base.metadata.create_all(engine)

if __name__ == '__main__':
    pass
