# -*- coding: utf-8 -*-
# <nbformat>3.0</nbformat>

# <codecell>

import pandas as pd
import pg8000
import psycopg2 as pq
import sqlalchemy as sa
from sqlalchemy import create_engine, ForeignKey, MetaData
from sqlalchemy import Column, Date, Integer, String, Table
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.ext.automap import automap_base
from sqlalchemy.orm import Session
from sqlalchemy.orm import relationship, backref
import peewee as pw
from fuzzywuzzy import fuzz
from fuzzywuzzy import process
from fuzzywuzzy.process import extract, extractBests,fuzz, itertools, extractOne
import etl  
from pprint import PrettyPrinter as Pretty
from collections import OrderedDict
import models
import postgresql


# <markdowncell>

# # Database Connection
# ____  
# 
# 
# 1. Create connection to the database with SQLalchemy
# 2. Instantiates meta as the extracted metadata from the schema **"public"**
# 3. Autoreflects the tables found in public and turns them into classes.
#     - The scope of which tables are reflected is defined in **"working_tables"** variable
# 4. Instantiates the Base class as an automapped Base (instead of a Base where tables are declared, in "declarative_base")
# 5. Activates the Base (with Base.prepare())
# 6. Creates global class names based on their respective counterparts which have been autoreflected and turned into models taht live in **Base.classes**
# 7. Sets the database to autocommit, and autoflush.Since we are using the <code>"sesion.merge"</code> method, this is a convenience for now. Eventually we'll make it a bit more secure.

# <codecell>



engine = create_engine('postgresql+pg8000://vassr:bluedog@localhost:5432/vassr')
meta = MetaData(schema="public")
session = Session(engine)
working_tables = ['person', 'location', 'condition_occurrence', 'care_site', 'observation', 'drug_exposure']
meta.reflect(engine, only=None)
Base = automap_base(metadata=meta)
Base.prepare()
Person, Condition, CareSite, Observation, DrugExposure, Location = Base.classes.person, Base.classes.condition_occurrence, Base.classes.care_site, Base.classes.observation, Base.classes.drug_exposure, Base.classes.location
session.autocommit = True
session.autoflush = True
ins = sa.inspect(engine)

person = Person()
condition = Condition()
caresite = CareSite()
obs = Observation()
drugexp = DrugExposure()
location = Location()


'''

# Old Code:

Base = automap_base()
engine = create_engine('postgresql+pg8000://vassr:bluedog@localhost:5432/vassr')
Base.prepare(engine, reflect=True)
help(Base.prepare)
'''
'''
meta = MetaData(schema="public")

meta.reflect(bind=engine, schema='person', views=False, only=None, extend_existing=False, autoload_replace=True)

meta.create_all(bind=engine)
Base = automap_base(metadata=meta)
Session = sa.orm.sessionmaker()
Session.configure(bind=engine)
session = Session()
Base.prepare(engine, reflect=True)

'''

# <codecell>


# <codecell>

d = {'personed': Person()}
x = d['personed']
x.attr_person_id = 11
x.attr_person_source_value = 'andrea'
session.merge(x)
#session.commit()


# <codecell>

printer.pprint(tablecheck)

# <codecell>

ss = session.query(Person)

# <codecell>

@sa.event.listens_for(Table, "column_reflect")
def column_reflect(inspector, table, column_info):
    # set column.key = "attr_<lower_case_name>"
    column_info['key'] = "attr_%s" % column_info['name'].lower()
    
class MyClass(Base):
    __table__ = Table("person", Base.metadata, autoload=True, autoload_with=engine)

# <codecell>

p.attr_person_id

# <codecell>

# Configure Environment

pretty = Pretty(indent=1)
pd.options.display.max_columns = 999
pd.options.display.max_rows = 999

# <codecell>

ihr.race.mapdict

# <markdowncell>

# # Convience Lists

# <codecell>

ins = sa.inspect(engine)
tablecheck = {}
for x in list(working_tables):
    for y in insp.get_columns(x):
        if y['name'] in list(tablecheck.keys()):
            print(y['name'] + " has duplicates")
            tablecheck[y['name']].append(str(x))
        else:
            tablecheck.update({y['name']:[str(x)]})

# <headingcell level=1>

# Classes

# <codecell>

# 11/19/14 - TODO
# FIX POSTMAP
# Figure out object oriented alignment so each ELEMENT can update each field into omop. It could just be raw swl..


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
        self.create_ids()
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
    def create_ids(self):
        '''creates a column with IDs conforming to GRDR standards'''
        source_tmp = input('Name of patient ID column in source data: ')
        source_id = closest_match(source_tmp, self.data.columns)
        guid = closest_match('GUID', self.data.columns)
        idcol = closest_match(source_id, self.data.columns)
        self.data['source_id_etl'] = self.data[idcol].apply(lambda x: str(str(self.regname) + '_' + str(x)))
        self.data['person_source_value'] = self.data[guid].fillna(self.data['source_id_etl'])
        self.person_source_value = self.data['person_source_value'].values
        self.data = self.data.set_index('person_source_value')
        return self.data
    
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
        self.target_table = input("What is the target table name?")
        self.target_field = input("What is the target field?")
        self.source_field = input("Name of omop source (eg condition_source_value)?")
        # type_concept_field = input("Name of omop type concept (eg condition_type_concept_id)?")
        self.postmap[self.target_field] = self.data.apply(self.mapper)
        self.postmap = self.postmap.rename(columns = {self.elementname: self.target_field})
        self.postmap[self.source_field] = self.data
        setattr(self,'mappingstatus', 'Postmap')
        # df_new = pd.DataFrame(self.postmap).rename(columns={self.elementname: self.target_field})
        # self.postmap = df_new
        #getattr(getattr(self, regname), 'postmap')[self.target_field] = getattr(self, 'postmap')
        #exec("%s.postmap = pd.concat([%s.postmap, self.postmap], axis=1)" % (self.regname, self.regname))
        print('Mapped %s: type = %s, target_table = %s, target_field = %s' %  (self.elementname, self.mappingtype, self.target_table, self.target_field)) 
        return self.postmap.fillna('null') 
    
    def direct_transform(self):
        for x in self.data.iteritems():
            if is_year(x):
                #pd.DataFrame(self.data * self.data.apply(is_year)).apply(pd.to_datetime, infer_datetime_format=True, format='%Y')
                self.data = ihr.dx_date.data.apply(pd.to_datetime, infer_datetime_format=True, format='%Y')
        return self.data.fillna('null')
      
        self.postmap[self.target_field] = pd.DataFrame(self.data)
    def __repr__(self):
        classrep = " %s.%s <class Element>" % (self.regname, self.sourcefield)
        print(" regname = '%s',\n elementname = '%s',\n sourcefield = '%s'" % (self.regname, self.elementname, self.sourcefield))
        if self.regname not in Registry.reglist:
            return "%s \n Warning: Origin name %s not an instantiated Registry" % (classrep, self.regname)
        else: return classrep 

# This amazing piece of programming will automatically map freetext into an omop concept id. Use if there is a lot of freetext.
default_target = 'GRDR_Mapping_11_13_14.xlsx'
class Mapper:
    def __init__(self, regobject, sheetname, target_file=default_target):
        self.regobject = regobject
        self.sheetname = sheetname
        self.target_file = target_file
        self.mapdf = pd.read_excel(target_file, sheetname=self.sheetname).fillna('null')
        self.regobject.mapdf = self.mapdf
        for x in range(0, len(self.mapdf)):
            value = self.mapdf.ix[x]['source_code']
            if type(value)==int and value in range(0,10):
                self.mapdf.loc[x, 'source_code'] = "%s %s" % (value, self.mapdf.loc[x, 'source_value'])
                #mapping['source_code'] + ' ' + mapping['source_value']
        self.mapmaster = self.mapdf.to_dict(orient="records")
        self.mapkeys = list(self.mapmaster[0].keys())
        
    def check_fields(self):
        self.goodfields = []
        count = 0
        for x in self.mapdf['field name'].dropna():
            if x in self.regobject.data.columns:
                print(x)
                self.goodfields.append(x)
                count +=1 
        print(str(count) + " fields extracted from the mapping table.")
        return self.goodfields
 
    def map_all(self):
        '''
       Map_all has a confusing array of variables. 
       - "mapkeys" are the columns of a mapping file. these are boilerplate -- 'field name', 'source code', 'source value', etc.
       - "mapdict_of_element" is the "mapdict" attribute of each Element object. so for ihr.race.mapdict, ihr is the registry, race is the element, and mapdict is the dictionary of valueset value to target mapping.'
       - "mapdict_of_element_keys" is a conveninence list that contains the KEYS of mapdict_of_element SANS all nan values. nan values trip up the fuzzy matching algorithm (extractOne), and it is definitely more valuable ot have that algorithm."
        '''
        for x in self.mapmaster:
            if x[closest_match('field_name', self.mapkeys)] in self.regobject.elements:
                
                mapdict_of_element= getattr(getattr(self.regobject, x['field name']), 'mapdict')
                mapdict_of_element_keys = [x for x in mapdict_of_element.keys() if str(x) != 'nan']
                print(mapdict_of_element)
                self.mapmaster[0][closest_match('yes', self.mapkeys)]

                code = x[closest_match('source_code', self.mapkeys)]
                value = x[closest_match('source_value', self.mapkeys)]
                try:
                    if process.extractOne(str(code),  mapdict_of_element_keys)[1] > 50:
                        try:
                            mapdict_of_element[code] = x[closest_match('omop_concept_id', self.mapkeys)]
                        except: handle_it()
                    else:
                        if process.extractOne(str(value),  mapdict_of_element_keys)[1] > 50:
                            try:
                                mapdict_of_element[value] = x[closest_match('omop_concept_id', self.mapkeys)]
                            except: handle_it()
                        print(str(x['field name']) + ", " + str(code) + " cannot be mapped")
                except:
                    handle_it()

class AutoMapper:
    def __init__(self, regobject):
        from algoliasearch import algoliasearch as ag
        self.client = ag.Client("31K5UZ93QX", "3fad605ef37e555d88993865e467f2a2")
        self.index = client.init_index('omop_concepts')
        self.regobject = regobject
        dic = {}

    # Alter this line right here to add the appropriate "unmappable" to be mapped. Also switch snomed to RxNorm, etc as appropriate
    def automap(self, element):
        for x in getattr(getattr(self.regobject, element), 'valueset'):
            try:
                length = len(x)
            except:
                next
            if len(x) > 1 and type(x) != int:
                res = self.index.search("\SNOMED\\ '%s'" % x, {"removeWordsIfNoResults":"firstWords", "facets":"CONCEPT_CLASS_ID", "facetFilters": "CONCEPT_CLASS_ID:Clinical Finding"})
                try:
                    result = int(res['hits'][0]['objectID'])
                except: pass
                ihr.dxc_secondaryto.mapdict[x] = result



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

# <headingcell level=1>

# Convenience Functions

# <codecell>

# Convenience functions
    
def handle_it():
    import sys
    e = sys.exc_info()[1]
    return e.args[0]


def is_year(x):
    if len(str(x)) == 4:
        if str(x)[:2] == '19' or  str(x)[:2] == '20':
            try:
                year = int(x)
            except:
                print("Looks like a year, but doesn't act like  one. Not budging to integer.")
        return True
    else: return False
    
def to_year(x):
    '''x needs to be an Element'''
    
    data = getattr(x, 'data')
    data.apply(pd.to_datetime, infer_datetime_format=True, format='%Y')

def reset(name_of_reg, target_file):
    ''' name_of_reg is three letter name. be SURE to also set the object variable to this same three letter name as well, as a convention. data is the registries flatfile.'''
    temp_reg = etl.Registry('name_of_reg', pd.read_sql_table(name_of_reg, engine, schema='sourcedata'))
    temp_reg.init_elements(checked)
    return temp_reg

def clean_mapper(mapping):
    lis = []
    for x in range(0, len(mapping)):
        value = mapping.ix[x]['source_code']
        if type(value)==int and value in range(0,10):
            mapping.loc[x, 'source_code'] = "%s %s" % (value, mapping.loc[x, 'source_value'])
            #mapping['source_code'] + ' ' + mapping['source_value']
    mapping = mapping.fillna('null')
    return mapping

def map_it():
    for x in ihr.mapmaster:
        if x['field name'] in ihr.elements:
            mapitem = getattr(getattr(ihr, x['field name']), 'mapdict')
            print(mapitem)
            code = x['source_code']
            value = x['source_value']
            if process.extractOne(code, list(mapitem.keys()))[1] > 50:
                try:
                    mapitem[code] = x['OMOP_ Concept_ID']
                except: pass
            else:
                if process.extractOne(value, list(mapitem.keys()))[1] > 50:
                    try:
                        mapitem[value] = x['OMOP_ Concept_ID']
                    except: pass
            if code not in mapitem.keys() and value not in mapitem.keys():
                print(str(x['field name']) + ", " + str(code) + " cannot be mapped")

def closest_match(x, choices):
    match = extractOne(x, choices)
    return match[0]

# <codecell>

pd.read_sql_table('person', engine)

# <codecell>

ihr = Registry('ihr', pd.read_csv('ihr/current_data/csv/ihr_complete.csv'))
ihr_map = Mapper(ihr,  'IHR Simplified')
checkfields = ihr_map.check_fields()
checked = ['race', 'ethnic', 'education', 'employment', 'dx_date', 'diamox', 'lasix', 'topamax', 'neptazane', 'dxc_confirm','dxc_diagnosis','dxc_secondaryto', 'dxc_reviewdate']
#ihr = reset('ihr', 'ihr/current_data/csv/ihr_complete.csv')
ihr.init_elements(checked)
ihr_map.map_all()
count = 1
def clean_up_array(array):
    for x in array:
        count +=1
        # person.attr_person_id = count
        person.attr_person_source_value = str(x)
        session.add(person)


# <codecell>

for x in list(ihr.person_source_value):
    person.attr_person_source_valuen = str(x)
    session.add(person)
    session.commit()

# <codecell>

x = pd.DataFrame(ihr.person_source_value).apply(str.replace(" ", ""))
x.values

# <codecell>


# <codecell>

mapping.mapmaster[500][closest_match('asdfad', mapping.mapkeys)]

# <codecell>

ihr.data.set_index(['person_source_value', 'Atitle_x'])

# <codecell>

mapping.mapmaster[0][closest_match('yes', mapping.mapkeys)]

# <codecell>

namhrload = etl.IO('mhr', 'namhr/current_data/20141029MHRegsitry.xlsx')
mhrdata = namhrload.xls_to_df()



mhr = etl.Registry('mhr', mhrdata)

mhr.mapmaster = open_mapping_sheet('namhr')

with open('namhr/current_data/csv/20141029MHRegsitry.csv', 'r') as f:
    wha = f.readlines()

mapmaster = mapping.to_dict(orient="records")


auto = AutoMapper(ihr)
auto.automap(ihr.dxc_secondaryto.elementname)




pd.read_sql_query("select * from person;", engine)

# <codecell>

ihr.mapmaster = mapping.to_dict(orient="records")

ihr.race.transform()

# <codecell>


            
   # def update(self):
    #    cols = list(self.registry.postmap.columns)
     #   comma = ","
      #  for x in self.registry.postmap.index:
       #     row = self.registry.postmap.ix[x]
        #    rowdict = row.to_dict()
         #   for col in cols:
          #      setter = "set %s = %s" % (col, rowdict[col])
           #     db.execute("update public.person %s where public.%s like '%s';" % (setter, 'person_source_value', str(x)))   


# <codecell>


loader = DBLoader(db, ihr.race, 'person')


update = db.prepare("update %s set %s = (select $1::varchar) where person_source_value::text like $2::text;" % (self.target_table, self.target_field))


loader.update_all()

db.bind('postgres', user='', password='', host='', database='')

# <codecell>

from pony.orm import *
db = Database()
db.bind('postgres', user='vassr', password='bluedog', host='localhost', database='vassr')

# <codecell>


Base = declarative_base(metadata=MetaData(bind=engine))

class MyTable(Base):
    __table__ = sa.Table('person', Base.metadata, autoload=True)
    
ry =  MyTable()


# <codecell>

tablename = 'person'
fieldname = 'race_source_value'
x = 123
typer = db.prepare("select data_type from information_schema.columns  where table_name = '%s' and column_name = '%s'" % (tablename, fieldname))
v1 = typer()[0][0]
ins = db.prepare("update %s set %s = (select %s::%s) where person_source_value::text like $1::text;" % (tablename, fieldname,x, v1))

# <codecell>

typedict = {'character':'',
'time with time zone':'',
'date':'',
'timestamp with time zone':'',
'smallint':'',
'character varying':'',
'boolean':'',
'double precision':'',
'integer':'',
'numeric':'',
'text':'',
'bigint':''}

# <codecell>

dd = pd.DataFrame(ihr.dx_date.data * ihr.dx_date.data.apply(is_year)).apply(pd.to_datetime, infer_datetime_format=True, format='%Y')
ihr.dx_date.data = ihr.dx_date.data.apply(pd.to_datetime, infer_datetime_format=True, format='%Y')

# <codecell>

for x in ihr.dx_date.data.iteritems():
    if not is_year(x):
        print(x)

# <headingcell level=3>

# is_year('1923')

# <codecell>

if is_year(11):
    print('no one sees')

# <codecell>

ihr.dx_date.data

# <codecell>

pd.set_option('display.max_rows', None)

# <codecell>

from random import randint
df = pd.DataFrame(randint(3, 8), index=['A', 'B', 'C'], columns=index)

# <codecell>


