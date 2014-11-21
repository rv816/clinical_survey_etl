
# coding: utf-8

# In[11]:

import pandas as pd
import sys
sys.prefix
import re
from pprint import pprint
from collections import OrderedDict
from itertools import islice
# postgresql+pypostgresql://user:password@host:port/dbname[?key=value&key=value...]


# In[12]:

class IO:
    '''IO Tranformer csv files, managing any transformations that need to happen when inputting or outputting a csv file. Has built in index renamer--attaching regname to beginning of index. '''
    def __init__(self, regname, targetfile):
        self.regname = regname
        self.targetfile = targetfile
    def xls_to_df(self):
        ex = pd.ExcelFile(self.targetfile)
        df = ex.parse()
        l = []
        for x in df.columns:
            p = re.sub('/','or', re.sub(' ', '_',x))
            l.append(p.lower())
        df.columns = l
        return df
    def csv_to_df(self):
        try:
            with open(self.targetfile, 'r') as infile:
                df = pd.read_csv(infile, index_col=0, header=0)
                l = []
                for x in df.columns:
                    p = re.sub('/','or', re.sub(' ', '_',x))
                    l.append(p)
                df.columns = l
                try:
                    int(excel.index[0])
                    excel.index = [self.regname + str(x) for x in excel.index]
                    print("Index was numeric -- %s prepended to beginning of each primary key" % self.regname)
                except:
                    pass
                return excel
                self.data = excel
        except:
            return "Pandas CSV IO tool failed. Check that targetfile is a CSV."
    def __repr__(self):
        return "<class IO_Csv(regname='%s', targetfile='%s')>" % (self.regname, self.targetfile)


# In[1]:

class Registry:
    '''Instantiate a registry for mapping. Only initiate with ready to go data. otherwise, work with IO object until properly formatted (headers in row 1, properly labeled index in first column, etc) '''
    reglist = []
    def __init__(self, regname, data, postmap = pd.DataFrame(), *args, **kwargs):
        self.regname = regname
        #exec("M.%s = self" % regname)
        self.data = pd.DataFrame(data)
        try:
            self.data.to_sql(self.regname, engine, if_exists='replace', index=True, index_label='source_id')
        except: print("Regsitry instance %s created, but could not add it to database." % self.regname)
        self.postmap = pd.DataFrame()
        self.elements = []
    def init_elements(self, elementnames, **kwargs): 
        ''' Warning: **kwargs will apply to all element names in elementnames list, if you choose to run init_elements on a list of elementnames. It is best for initiating several elements that have similar characteristics.'''
        for x in list(elementnames):
            self.elements.append(x)
            print("Parsing " + x + "....")
            valueset = list(self.data[x].drop_duplicates().values)
            setattr(self, x, Element(x, valueset, self.data[x], self.regname))
            # exec("self.%s = Element('%s', valueset, self.data['%s'], self.regname)" % (x,x,x))
            #for key, value in kwargs.items():
             #   exec("self.%s.%s = '%s'" % (x, key, value))
              #  print("%s, %s" % (key,value))
            # exec("if self.%s.mappingtype == 'direct':" % x)
            # if self.Year_of_Birth.mappingtype == 'direct': 
            # exec("for value in self.%s.valueset:" % x)
            setattr(getattr(self, x), 'mapdict', {})
            for value in getattr(getattr(self, x), 'valueset'):
                print("Element = " + str(x) + ", Value = " + str(value))
                getattr(getattr(self, x), 'mapdict')[value] = 'null'
                    
           # exec("M.masterplan[%s] = self.elements" % self.regname)
        self.datadict = {}
        for x in self.elements:
            self.datadict[x] = list(self.data[x].drop_duplicates().values)
        Registry.reglist.append(self.regname)
    def __repr__(self):
        return "< Registry '%s' >" % self.regname
    


# In[4]:

class Element:
    def __init__(self, elementname, valueset, data, regname, mappingtype='', mappingstatus='Premap', mapdict={}, postmap = pd.DataFrame(), target_table='Not yet specified', target_field='Not yet specified',  *args, **kwargs):
        self.elementname = elementname
        self.regname = regname # source registry name
        self.sourcefield = elementname # source column
        self.valueset = list(valueset) # source value
        self.postmap = postmap
        self.data = data
        self.regname = regname
        self.valueset = list(valueset)
        self.mappingtype = mappingtype
        self.mappingstatus = mappingstatus
        self.mapdict = mapdict
        self.target_table = target_table
        self.target_field = target_field
    def mapper(self, x):
            try:
                return self.mapdict[x]
            except: return 'null'
    def transform(self):
        # Where x is the each value of the original data, look up the value for key x in Element.mapdict
        target_table = input("What is the target table name?")
        target_field = input("What is the target field?")
        source_field = input("Name of source field?")
        self.postmap = pd.DataFrame(self.data).applymap(self.mapper).rename(columns={self.elementname: target_field})
        self.postmap[source_field] = self.data
        setattr(self,'mappingstatus', 'Postmap')
        # df_new = pd.DataFrame(self.postmap).rename(columns={self.elementname: self.target_field})
        # self.postmap = df_new
        #getattr(getattr(self, regname), 'postmap')[self.target_field] = getattr(self, 'postmap')
        #exec("%s.postmap = pd.concat([%s.postmap, self.postmap], axis=1)" % (self.regname, self.regname))
        print('Mapped %s: type = %s, target_table = %s, target_field = %s' %  (self.elementname, self.mappingtype, self.target_table, self.target_field)) 
        return self.postmap 
    def __repr__(self):
        classrep = " %s.%s <class Element>" % (self.regname, self.sourcefield)
        print(" regname = '%s',\n elementname = '%s',\n sourcefield = '%s'" % (self.regname, self.elementname, self.sourcefield))
        if self.regname not in Registry.reglist:
            return "%s \n Warning: Origin name %s not an instantiated Registry" % (classrep, self.regname)
        else: return classrep 


class DBLoader:
    def __init__(self, db, registry, target_table, target_field='NO_FIELD'):
        self.db = db
        self.registry = registry
        self.target_table = target_table
        self.target_field = target_field
        self.data = registry.data
        self.insert_id = db.prepare("insert into person (person_source_value) select $1 AS varchar WHERE NOT EXISTS (SELECT 1 FROM person WHERE person_source_value like '$1')")
        self.clean_duplicates = db.prepare("""
               DELETE FROM person USING person p2
              WHERE person.person_source_value = p2.person_source_value AND person.person_id < p2.person_id;
              """)
    def insert_all_ids(self):
        for x in self.data.index:
            try:
                self.insert_id(str(x))
                self.clean_duplicates()
                print("Added " + str(x) + " to vassr.public.person.person_source_value")
            except Exception:
               print(handle_it())
    def update_all(self):
        cols = list(self.registry.postmap.columns)
        for x in self.registry.postmap.index:
            print(x)
            row = self.registry.postmap.ix[x]
            rowdict = row.to_dict()
            self.target_field = cols[0]
            for col in cols:
                self.target_field = col
                typer = db.prepare("select data_type from information_schema.columns  where table_name = '%s' and column_name = '%s'" % (tablename, fieldname))
                v1 = typer()[0][0]
                if rowdict[col] != '':
                    print("col = " + str(col) + ", rowdict[col] = " + str(rowdict[col]))
                    ins = db.prepare("update %s set %s = (select '%s'::%s) where person_source_value::text like '%s';" % (tablename, fieldname,str(rowdict[col]), v1, x))
                    ins()
        return pd.read_sql_table(self.target_table, engine)

pd.set_option('max_columns', None)
pd.set_option('max_colwidth', 1000)
pd.set_option('max_rows', None)


# In[ ]:



