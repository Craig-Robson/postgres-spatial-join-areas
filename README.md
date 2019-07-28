# postgres spatial join for areas
Script for running spatial joins between datasets areas.

## description
A simple module which allows for the running of spatial join in postgres utilising the postgis extension for a number of data scenarios and join outcomes - (a) a list of the polygons each feature intersects with and (b) a single polygon a feature intersects with. Results are stored as part of the dataset in the database in new columns. In the case of (a), results are stored using the postgres 'character varying[]', resulting in an array of values which can be quireied.

With (a) where the dataset of itnerest is polygons, and the join dataset is polygons, the 'coverage', the overlap between the polygons is recorded and returned also. For points and polylines only the id of the intersecting areas is returned. 

## parameters
**dataset:** the name of the table in postgres the areas are to be join to  
**join_multiple_areas:** if all areas which insterect those features in the dataset are to be recorded set as True. default is true  
**areas_to_join:** the name of the table in postgres for the areas which are to be joined  
**join_data_is_areas:** set as true if the dataset is polygons. set as false if polyines or points. default is True   
**fields_to_join:** not currently active  
**dataset_id_field:** the column in the dataset which can be used as the unique identifier. default is 'gid'  
**join_areas_id_field:** the column in the table of the data to be joined which is a unique identifier and will be copied into the dataset as a result of the join. default is 'gid'  
**dataset_geom_field:** the name of the column in the dataset which stores the geomerty which should be used for the join. default is 'geom'  
**join_areas_geom_field:** then anme of the column in the area data which is to be joined which should be used. default is 'geom'  
**database_connection:** a psycopg2 live connection object to the database containing the tables to be joined. default is None. if passing, set connection.autocommit=True before passing
**connection_parameters:** a dictionary object wich contains the following keys and data for the database connection - 'database_name','user', 'password', 'port', 'host'

## dependencies
psycopg2  

