
# coding: utf-8

# In[1]:

get_ipython().magic('matplotlib inline')
from bs4 import BeautifulSoup
import pandas as pd
import dateparser
import missingno as msno
import re
import operator
import itertools
from string import capwords # trim, titlecase https://docs.python.org/3/library/string.html#string.capwords


# In[2]:

cols = ['source','year','name','dob','desc']


# # 2018 list

# In[3]:

def clean_name(string):
    return capwords(string.replace('.','')).title()


# In[4]:

# Extract from http://putin2018.ru/trusted/ html:
def extract_from_html(e):
    name = clean_name(e.select('a')[0].extract().text)
    desc = e.text.strip()
    return ("putin2018", None, name, None, desc)

with open("data/raw/putin2018_trusted.html") as f:
    soup = BeautifulSoup(f.read(), "lxml")
    entries = soup.select('li.person-by-alphabet-list_item')
    df_2018_putinru = pd.DataFrame([extract_from_html(e) for e in entries], columns=cols)
    df_2018_putinru['year'] = '2018'

print("putin2018.ru", len(df_2018_putinru))
df_2018_putinru.sample(5)


# # 2018 list from cikrf

# In[5]:

def cikrf_df(files, func):
    df = pd.DataFrame(columns=cols)
    for file in files:
        with open('data/raw/'+file) as f:
            text = f.read()
            text = text.replace('№ ', '№') # Make splitting easier
            #text = text.replace('–', '—') # ndash with mdash
            list_ = re.compile('\s\d+\.').split(text)
            list_ = list_[1:] # skip header

            df_ = pd.DataFrame([func(e) for e in list_], columns=cols)        
            df_['source'] = file.replace('.txt','')

            df = df.append(df_, ignore_index=True)
            print(file, '+', len(df_), '\t', len(df))

    return df


# In[6]:

def extract_2018_txt(el):
    name = clean_name(el)
    return (None, '2018', name, None, None)


# In[7]:

files2018 = ['126-1057-7.txt','131-1085-7.txt','134-1108-7.txt','144-1192-7.txt']
df_2018_cikrf = cikrf_df(files2018, extract_2018_txt)
df_2018_cikrf.sample(5)


# # 2012 list

# In[8]:

def extract_2012_txt(el):
    e = el.strip().replace('\n',' ').replace('\t',' ').replace('  ',' ')
    elements = e.replace(', дата рождения','').replace(' года, основное место работы ','').split('\uf02d')
    name = clean_name(elements[0])
    dob = dateparser.parse(elements[1])
    desc = elements[2].strip()
    return (None, '2012', name, dob, desc)


# In[9]:

# manually "cleaned"
files2012 = ['96-767-6.txt','98-785-6.txt','100-798-6.txt','107-858-6.txt']
df_2012_cikrf = cikrf_df(files2012, extract_2012_txt)
df_2012_cikrf.sample(5)


# In[10]:

cols_extended = ["source","name","dob","desc", # gathered
                 "wikidata","email","vk","ig","fb","tw","yt","ok","zampolit","sanctioned" # possible?
                ]

df_2012_cikrf.to_csv('data/csv/2012_cikrf.csv', index=False, columns=cols_extended)
df_2018_cikrf.to_csv('data/csv/2018_cikrf.csv', index=False, columns=cols_extended)
df_2018_putinru.to_csv('data/csv/2018_putinru.csv', index=False, columns=cols_extended)


# In[11]:

# Names only:
df_2012_cikrf.sort_values(by=['name'])['name'].to_csv('data/names/2012_cikrf_names.txt', index=False, header=False)
df_2018_cikrf.sort_values(by=['name'])['name'].to_csv('data/names/2018_cikrf_names.txt', index=False, header=False)
df_2018_putinru.sort_values(by=['name'])['name'].to_csv('data/names/2018_putinru_names.txt', index=False, header=False)


# # Merge Names

# In[20]:

df_merge = pd.concat([df_2012_cikrf, df_2018_cikrf, df_2018_putinru], ignore_index=True)

dupes = df_merge[df_merge.duplicated(['name'], keep=False)].sort_values(by=['name'])
print(len(dupes))
dupes.head(10)


# In[23]:

# Some manual replacements for known dupes:
replacements = {
    'Лагутинская Софья Владимировна': 'Лагутинская София Владимировна',
    'Ласицкене Мария Александровна': 'Ласицкене (Кучина) Мария Александровна',
    'Юнусов Тимур Ильдарович': 'Юнусов Тимур Ильдарович (Тимати)',
    'Якубов Февзи': 'Якубов Февзи Якубович',
    'Симонов Юрий Павлович': 'Симонов (Вяземский) Юрий Павлович',
    'Ооржак Ошку-Саар Аракчеевна': 'Ооржак Ошку-Саар Аракчааевна',
    'Назейкин Анатолии Георгиевич': 'Назейкин Анатолий Георгиевич',
    'Львова-Белова Мария Александровна': 'Львова-Белова Мария Алексеевна',
    'Гыштемулте Ефросиниа Николаевна': 'Гыштемулте Ефросиния Николаевна',
    'Вележова Лидия Леонидовна': 'Вележева Лидия Леонидовна'
}

df_merge['name'].replace(replacements, inplace=True)


# In[24]:

# can also be sorted(set()) but this works for unhashable types too
def sort_uniq(sequence):
    return map(operator.itemgetter(0), itertools.groupby(sorted(sequence)))

def str_notblank(val):
    return capwords(str(val)) != ''

# Sort, unique, merge values, return list, or none, or single value
def list_agg(vals):
    #list_ = sorted(set([v for v in vals if pd.notnull(v) and pd.notna(v) and str_notblank(v)]))
    list_ = list(sort_uniq([v for v in vals if pd.notnull(v) and pd.notna(v) and str_notblank(v)]))
    if len(list_) == 0:
        return None
    #print(len(list_), list_)
    if len(list_) == 1:
        return list_[0]
    return list_


# In[25]:

dedupe_merge = df_merge.groupby(['name']).agg(lambda x: tuple(x)).applymap(list_agg).reset_index()
print(len(dedupe_merge))
dedupe_merge.sample(10)


# In[26]:

# Missing Values
msno.matrix(dedupe_merge)


# In[27]:

dedupe_merge.sort_values(by=['name'])['name'].to_csv('data/names/merged_names.txt', index=False, header=False)


# In[29]:

dedupe_merge[pd.isna(dedupe_merge['desc'])]


# In[ ]:



