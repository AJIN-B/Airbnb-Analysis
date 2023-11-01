
# from pymongo.mongo_client import MongoClient
from pymongo import MongoClient
import datetime
print(datetime.datetime.now())

url = "Provide your mongodb url"
cluster = MongoClient(url)

try:
    cluster.admin.command('ping')
    print("Pinged your deployment. You successfully connected to MongoDB!")
except Exception as e:
    print(e)


#%% done

database_names = list(cluster.list_database_names())

db = cluster.get_database('sample_airbnb')
collection_names = list(db.list_collections())

collection = db.listingsAndReviews

data_1 = list(collection.find().limit(5))

#%% data retrival

every = list(collection.find({},{'bedrooms':1, 'listing_url':1, 'maximum_nights':1,
                                 'minimum_nights':1,'address':{'country_code':1,'market':1,'suburb':1,
                                'location':{'coordinates':1}},
                                 'amenities':1, 'bathrooms':1, 'bed_type':1,
                                 'cancellation_policy':1, 'extra_people':1, 
                                 'host':{'host_name':1,'host_neighbourhood':1 ,'host_id':1 ,'host_location':1}, 
                                 'number_of_reviews':1, 'price':1 ,'property_type':1 ,
                                 'room_type':1, "_id":1, 'name':1,
                                 'review_scores':{'review_scores_rating':1}}))


#%% changing to dict

import pandas as pd

def lop(x):
    temp = dict()
    for i in x:
        if isinstance(x[i], dict):
            l = lop(x[i])
            temp.update(l)
        else:
            temp[i] = x[i]
    return temp

data = list()
for i in range(len(every)):
    temp = dict() 
    val = every[i]
    for k in val:
        val_k = val[k]
        if isinstance(val_k, dict):
            l = lop(val_k)
            temp.update(l)
        else:
            temp[k] = val_k
    data.append(temp)


# longitude and latitude
for i in range(len(data)):
    cor = data[i]['coordinates']
    data[i].update({'longitude':cor[0],"latitude":cor[1]})
    

#%% dataframe convert

d  = pd.DataFrame(data=data)

# ['_id', 'listing_url', 'name', 'property_type', 'room_type', 'bed_type', 'minimum_nights', 
# 'maximum_nights', 'cancellation_policy', 'bedrooms','number_of_reviews', 'bathrooms',
# 'amenities', 'price', 'extra_people','host_id', 'host_name', 'host_location', 'market',
# 'host_neighbourhood', 'suburb', 'coordinates', 'review_scores_rating', 'latitude', 'longitude']

#%% save

save_df = d.copy()
col = ['listing_url','bed_type','cancellation_policy','bathrooms','amenities',
       'extra_people','host_location', 'host_neighbourhood', 'suburb', 'coordinates']

save_df = save_df.drop(columns=col)

save_df[['maximum_nights','minimum_nights']] = save_df[['maximum_nights','minimum_nights']].astype(str).astype(float)
save_df[['number_of_reviews','price']] = save_df[['number_of_reviews','price']].astype(str).astype(float)

# ['_id', 'name', 'property_type', 'room_type', 'minimum_nights', 'maximum_nights', 
# 'number_of_reviews', 'price', 'host_id', 'host_name', 'market', 'country_code', 
# 'review_scores_rating', 'latitude', 'longitude']

# save_df.to_excel("aribnb_data.xlsx")


#%%

dat = pd.read_csv('AB_NYC_2019.csv')

dat['reviews_per_month'].fillna(0,inplace=True)
dat['last_review'].fillna(dat['last_review'].mode().values[0],inplace=True)

dat.dropna(axis=0,inplace=True) 

# dat.to_excel("AriBNB data.xlsx") 


