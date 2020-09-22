CREATE INDEX Days_Open_Index ON Days_Open(business_id, open, close);
CREATE INDEX Business_Index ON Businesses(city,state);
CREATE INDEX Reviews_Index ON Reviews(business_id);

-- Oracle creates the following indices during table creation because the index columns are the table's primary key:
--CREATE INDEX Main_Category_Index ON Main_Categories(business_id, category_name);
--CREATE INDEX Sub_Category_Index ON Sub_Categories(business_id, category_name);
--CREATE INDEX Business_Attributes_Index ON Business_Attributes(business_id, attribute);
