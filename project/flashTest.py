from sqlalchemy import create_engine, Column, Integer, String, Text
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker

DB_URI = 'postgresql+psycopg2://OE_User:JGP123456@CNWXIM0WINSVC01:5438/Trace_T1'
engine = create_engine(DB_URI)
Base = declarative_base(engine)
session = sessionmaker(engine)()

class Article(Base):
    __tablename__ = "actiontracker_faca"
    item_id = Column(String(10), primary_key=True,autoincrement=True)
    fa = Column(String(150))
    ca = Column(String(150))
    returndri = Column(String(50))
    returntime = Column(String(50))

    def __str__(self):
        return '[%s, %s, %s, %s, %s]' % (self.item_id, self.fa, self.ca, self.returndri, self.returntime)
        return '[%s, %s, %s]'% (self.item_id, self.fa, self.ca)

    def select_data():
        # 查询所有数据
        all_article = session.query(Article).filter_by(fa=None, ca=None).all()
        all_article = session.query(Article).all()
        print('------------')
        for p in all_article:
            print(p)
        print('数据查询成功')

    def add_data():
        article1 = Article(item_id=1, fa="jiaojiao1", ca="content1")
        article2 = Article(item_id=1,fa="jiaojiao2", ca="content2")
        article3 = Article(item_id=1,fa="jiaojiao3", ca="content3")
        session.add_all([article1, article2, article3])
        session.commit()
        print('数据添加成功')

    def delete_data():
        article = session.query(Article).filter_by(fa="jiaojiao3").all()
        art = session.query(Article).filter_by(fa="jiaojiao3").delete()
        session.commit()
        print('数据删除成功')

    def update_data():
        upda = {Article.fa:"待回复", Article.ca:"待回复" }
        itemid = {Article.fa: "待回复", Article.ca: "待回复",
                  Article.returndri: "待回复",
                  Article.returntime: "待回复"}
        article = session.query(Article).filter_by(fa=None,ca=None).update(itemid)
        session.commit()
        print('数据更新成功')

if __name__ == '__main__':
    Article.select_data()
    Article.update_data()
    Article.select_data()