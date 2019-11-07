import psycopg2
from psycopg2 import sql


def create_database_connection(db_params):
    """
    Establish database connection to postgres
    """

    connection = psycopg2.connect("dbname=%s user=%s port=%s host=%s password=%s" %(db_params['database_name'], db_params['user'], db_params['port'], db_params['host'], db_params['password']))

    return connection


def main(database_connection=None, connection_parameters=None, dataset='', join_multiple_areas=True, areas_to_join='', join_data_is_areas=True, fields_to_join=[], dataset_id_field='gid', join_areas_id_field='gid', dataset_geom_field='geom', join_areas_geom_field='geom'):
    """
    Run the spatial join over two tables existing in a postgres database.

    fields_to_join:
     - [{name:xxx, type:xxx},{name:yyy, type:yyy}]
    """
    print('In main')
    if database_connection is None:
        if connection_parameters is None:
            # return an error to the user
            return 'fail'
        database_connection = create_database_connection(connection_parameters)

    # allow all calls to be committed when they are run
    database_connection.autocommit = True

    # create cursor to access database
    cursor = database_connection.cursor()

    temp_table = '___temp'

    if join_multiple_areas:
        # run join
        if join_data_is_areas:
            cursor.execute(sql.SQL('SELECT b.{0} as gid, ARRAY_AGG(st_area(st_intersection(b.{1}, t.{2})) / st_area(b.{3})) as coverage, ARRAY_AGG(t.geo_code) as lads, ARRAY_AGG(t.geo_code_gor) as gors INTO {9} FROM ftables.{4} t, {5} b WHERE st_intersects(b.{6}, t.{7}) GROUP BY b.{8};').format(
                sql.Identifier(dataset_id_field),  # 0
                sql.Identifier(dataset_geom_field),  # 1
                sql.Identifier(join_areas_geom_field),  # 2
                sql.Identifier(dataset_geom_field),  # 3
                sql.Identifier(areas_to_join),  # 4
                sql.Identifier(dataset),  # 5
                sql.Identifier(dataset_geom_field),  # 6
                sql.Identifier(join_areas_geom_field),  # 7
                sql.Identifier(dataset_id_field),  # 8
                sql.Identifier(temp_table)),  # 9
                [])
        else:
            #print('Running line & point join')
            #cursor.execute(sql.SQL('SELECT b.{0} as gid INTO {2} FROM {1} b;').format(sql.Identifier('gid'), sql.Identifier(dataset), sql.Identifier('_temp')))
            cursor.execute(sql.SQL('SELECT b.{0} as gid, ARRAY_AGG(t.geo_code) as lads, ARRAY_AGG(t.geo_code_gor) as gors INTO {5} FROM ftables.{1} t, {2} b WHERE st_intersects(b.{3}, t.{4}) GROUP BY b.{6};').format(
		sql.Identifier(dataset_id_field), sql.Identifier(areas_to_join), sql.Identifier(dataset), sql.Identifier(dataset_geom_field), sql.Identifier(join_areas_geom_field), sql.Identifier(temp_table), sql.Identifier(dataset_id_field)))

        #print('Generated temp table')
        # create fields in dataset to join areas to
        #for field in fields_to_join:
        cursor.execute(sql.SQL('ALTER TABLE {} ADD IF NOT EXISTS lads character varying[];').format(sql.Identifier(dataset)))
        cursor.execute(sql.SQL('ALTER TABLE {} ADD IF NOT EXISTS gors character varying[];').format(sql.Identifier(dataset)))

        # add field for coverage values if join involves areas
        if join_data_is_areas:
            cursor.execute(sql.SQL('ALTER TABLE {} ADD IF NOT EXISTS lad_coverage float[];').format(sql.Identifier(dataset)))

        # create index on the ids to speed up the update
        #temp = temp_table+'_gid_idx'
        cursor.execute(sql.SQL('CREATE INDEX {0} ON {1} USING btree (gid);').format(sql.Identifier(temp_table+'_gid_idx'), sql.Identifier(temp_table)), [])
        cursor.execute(sql.SQL('CREATE INDEX IF NOT EXISTS {0} ON {1} USING btree (gid);').format(sql.Identifier(dataset+'_gid_idx'), sql.Identifier(dataset)), [])

        # run the update
        cursor.execute(sql.SQL('UPDATE {0} as a SET lads = b.lads, gors = b.gors FROM {1} b WHERE a.{2} = b.gid;').format(sql.Identifier(dataset), sql.Identifier(temp_table), sql.Identifier(dataset_id_field)))

        # remote the temp tables
        cursor.execute(sql.SQL('DROP INDEX IF EXISTS {0};').format(sql.Identifier(temp_table+'_gid_idx')), [])
        cursor.execute(sql.SQL('DROP TABLE IF EXISTS {0};').format(sql.Identifier(temp_table)))

        # create index on new fields which are now populated
        cursor.execute(sql.SQL('CREATE INDEX IF NOT EXISTS {0} ON {1} USING gin(lads);').format(sql.Identifier(dataset+'_lads_idx'), sql.Identifier(dataset)), [])
        cursor.execute(sql.SQL('CREATE INDEX IF NOT EXISTS {0} ON {1} USING gin(gors);').format(sql.Identifier(dataset+'_gor_idx'), sql.Identifier(dataset)), [])

    else:
        # if join result is to find a single area rather than multiple
        # create fields in dataset to join areas to
        # for field in fields_to_join:
        cursor.execute(sql.SQL('ALTER TABLE {} ADD IF NOT EXISTS lad character varying;').format(sql.Identifier(dataset)))
        cursor.execute(sql.SQL('ALTER TABLE {} ADD IF NOT EXISTS gor character varying;').format(sql.Identifier(dataset)))

        # run spatial join
        cursor.execute(sql.SQL('UPDATE {0} a SET lad = b.lad_code, gor = b.gor_code FROM ftables.{1} b WHERE ST_Intersects(b.{2}, a.geom);').format(sql.Identifier(dataset), sql.Identifier(areas_to_join), sql.Identifier(dataset_geom_field)))

    return True

print('here')