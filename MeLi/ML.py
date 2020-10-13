#!/usr/bin/env python
# coding: utf-8

# In[1]:


#importacion de modulos
import json
import pandas as pd
import numpy as np


# In[4]:


#Carga de JSON
dblist = json.loads(open('./Datos_Externos/dblist.json').read())


# In[5]:


dblist_df = pd.DataFrame(dblist)
dblist_df.head(10)


# In[6]:


#revision de diseño JSON
print(json.dumps(dblist, indent=4))


# In[7]:


#Corrijo Json y lo levanto
dblist = json.loads(open('./Datos_Externos/dblist_v2.json').read())


# In[8]:


dblist_df = pd.DataFrame(dblist)
dblist_df.head(10)


# In[9]:


#revision de diseño JSON
print(json.dumps(dblist, indent=4))


# In[10]:


results=dblist["db_list"]


# In[11]:


df=pd.DataFrame(results)
df.head()


# In[12]:


df["classification_confidentiality"]= np.nan
df["classification_integrity"]= np.nan
df["classification_availability"]= np.nan
df["owner_name"]= np.nan
df["owner_uid"]= np.nan
df["owner_email"]= np.nan






# In[13]:


df_classification = df["classification"]
df_owner = df["owner"]


# In[14]:


for i in range (len(df)):
    print (df_classification[i]['confidentiality'])


# In[15]:


for i in range (len(df)):
    print (df_classification[i]['integrity'])


# In[16]:


for i in range (len(df)):
    print (df_classification[i]['availability'])


# In[17]:


for i in range (len(df)):
    print (df_owner[i]['name'])


# In[18]:


for i in range (len(df)):
    print (df_owner[i]['uid'])


# In[19]:


#error en estructura de JSON
for i in range (len(df)):
    print (df_owner[i]['email'])
    


# In[20]:


for i in range (len(df)):
    df["classification_confidentiality"][i]= (df_classification[i]['confidentiality'])
    df["classification_integrity"][i]= (df_classification[i]['integrity'])
    df["classification_availability"][i]= (df_classification[i]['availability'])
    df["owner_name"][i]= (df_owner[i]['name'])
    df["owner_uid"][i]= (df_owner[i]['uid'])
    df["owner_email"][i]= (df_owner[i]['email'])
    


# In[21]:


df


# In[22]:


df2 = df[['dn_name','classification_confidentiality', 'classification_integrity', 'classification_availability', 'owner_name', 'owner_uid','owner_email']]
df2


# In[23]:


#Carga csv
manager = pd.read_csv("./Datos_Externos/user_manager.csv", encoding = 'latin1',names =['row_id', 'user_id', 'user_state', 'user_manager'])
manager


# In[24]:


#merge JSON con CSV
DB = pd.merge(df2,manager,left_on='owner_uid',right_on='user_id')
DB


# In[26]:


#Generacion base
DB.to_csv('./Bases/DB.csv')


# In[27]:


DB.loc[(DB['classification_confidentiality'] == "high")] 


# In[28]:


DB_mail = DB.loc[(DB['classification_confidentiality'] == "high") | (DB['classification_integrity'] == "high") | (DB['classification_availability'] == "high")][['dn_name', 'owner_name','owner_email','user_manager','classification_confidentiality','classification_integrity','classification_availability']]
DB_mail


# In[29]:


#Generacion base para envio de mail
DB_mail.to_csv('./Bases/DB_mail.csv')


# In[ ]:




