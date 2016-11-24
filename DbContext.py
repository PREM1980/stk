import MySQLdb
import itertools

class DbContext(object):
      def __init__(self):
         print "New connection object created"
         # self.db = MySQLdb.connect("prod-api-box","ninja","ninja!@#$","vbo" )         
         self.db = MySQLdb.connect("localhost","ninja","ninja!@#$","vbo" )         
         # self.db = MySQLdb.connect("96.118.52.175","ninja","ninja!@#$","sid1" )         
         self.cursor = self.db.cursor()

      def execute(self,qry, value):          
          try:
              self.cursor.execute(qry, value)
          except MySQLdb.Error as e:
              print "Error executing - execute query ", qry, e
              raise Exception('DbContext Execute exception - qry-executed == {0} and value == {1}'.format(qry, value))
          
          results = self.dict_gen(self.cursor)          
          self.db.commit()
          return results

      def executemany(self,qry, values):
          results = ''
          try:
              results = self.cursor.executemany(qry, values)
          except MySQLdb.Error as e:              
              print "DbContext Error executing - executemany query ", e
              raise Exception('DbContext Execute exception - qry-executed == {0} and value == {1}'.format(qry, values))

          self.db.commit()
          return results

      def dict_gen(self,curs):
          field_names = [d[0].lower() for d in curs.description]
          while True:
              rows = curs.fetchmany()
              if not rows: return
              for row in rows:

                yield dict(itertools.izip(field_names, row))

      def close(self):
          print 'Database Connection closed and you prevented memory leaks'
          self.db.close()

def decimal_default(obj):
    if isinstance(obj, decimal.Decimal):
        return float(obj)
    raise TypeError
