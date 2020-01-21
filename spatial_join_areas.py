import psycopg2
from psycopg2 import sql


def create_database_connection(db_params):
    """
    Establish database connection to postgres
    """

    connection = psycopg2.connect("dbname=%s user=%s port=%s host=%s password=%s" %(db_params['database_name'], db_params['user'], db_params['port'], db_params['host'], db_params['password']))

    return connection


def check_data_exists(database_connection, data_name):
    """
    Check the named dataset exists. Returns True if exists, False if not.
    """
    # create cursor to access database
    cursor = database_connection.cursor()

    # run query to see if dataset exists
    cursor.execute(sql.SQL('SELECT EXISTS(SELECT 1 FROM information_schema.tables WHERE table_name = {});').format(sql.Literal(data_name)))

    res = cursor.fetchall()[0][0]

    cursor.close()
    return res


def check_fields_to_join(fields_to_join):
    """Check which fields have been passed to be joined
    """
    if 'oa' in fields_to_join: oa = True
    else: oa = False

    if 'lad' in fields_to_join: lad = True
    else: lad = False

    if 'gor' in fields_to_join: gor = True
    else: gor = False

    return oa, lad, gor


def get_srid(database_connection, table, geom_field):
    """Get the SRID of a dataset
    """

    # create cursor to access database
    cursor = database_connection.cursor()

    # run query to get srid
    cursor.execute(sql.SQL('SELECT srid FROM public.geometry_columns WHERE f_table_name={};').format(sql.Literal(table)))

    # fetch query result
    res = cursor.fetchall()

    # if more than one row returned, return an error
    if len(res) > 1:
        cursor.execute(sql.SQL('SELECT srid FROM public.geometry_columns WHERE f_table_name={} and f_geometry_column={};').format(sql.Literal(table), sql.Literal(geom_field)))

        # fetch query result
        res = cursor.fetchall()

        if len(res) > 1:
            return 'More than one table with the same name: %s' % res

    # convert returned srid into an integer
    srid = int(res[0][0])

    # close cursor
    cursor.close()

    # return
    return srid


def check_the_srid_of_the_data(database_connection, dataset_name, dataset_geom_field, join_dataset, join_dataset_geom_field):
    """Check the SRID of both geometries is the same
    """
    # get srid of dataset
    srid_data = get_srid(database_connection, dataset_name, dataset_geom_field)

    # check the returned srid is not an error string
    if isinstance(srid_data, str):
        return False, srid_data

    # get srid of areas to join
    srid_join_areas = get_srid(database_connection, join_dataset, join_dataset_geom_field)

    # check the returned srid is not an error string
    if isinstance(srid_data, str):
        return False, srid_data

    # compare srid's - return an error if they are not equal
    if srid_data != srid_join_areas:
        return False, 'The SRIDs for the two datasets do not match - they are required to.'

    return True


def main(database_connection=None, connection_parameters=None, dataset='', join_multiple_areas=True, areas_to_join='', join_data_is_areas=True, fields_to_join=[], dataset_id_field='gid', join_areas_id_field='gid', dataset_geom_field='geom', join_areas_geom_field='geom'):
    """
    Run the spatial join over two tables existing in a postgres database.

    fields_to_join:
     - [{name:xxx, type:xxx},{name:yyy, type:yyy}]
    """

    # check if a connection has been passed
    if database_connection is None:

        # if no connection, check database parameters has been passed
        if connection_parameters is None:
            # return an error to the user if parameters haven't been passed either
            return False, 'No database connection passed or database connection parameters. At least one should be sent.'

        # create a database connection using the passed parameters
        database_connection = create_database_connection(connection_parameters)

    # allow all calls to be committed when they are run
    database_connection.autocommit = True

    # check the two datasets exist
    exists = check_data_exists(database_connection, dataset)

    if not exists:
        return "Error! Could not fine the dataset %s" % dataset

    exists = check_data_exists(database_connection, areas_to_join)
    if not exists:
        return "Error! Could not fine the join dataset %s" % areas_to_join

    # check the two datasets have matching srid's
    matching_srids = check_the_srid_of_the_data(database_connection, dataset, dataset_geom_field, areas_to_join, join_areas_geom_field)

    # if false, return an error to the user
    if matching_srids is not True:

        # get the length of the matching_srids - if greater than 1 it includes an error string
        if len(matching_srids) > 1:
            # An problem has been found when checking the SRIDs of the datasets
            return 'Error!. %s' % matching_srids[1]
        else:
            # a generic error has occurred
            return 'The two datasets have different SRIDs. Please correct this and try again'

    # check to see what fields have been passed
    oa, lad, gor = check_fields_to_join(fields_to_join)

    # create cursor to access database
    cursor = database_connection.cursor()

    temp_table = '___temp'

    # delete temp table if it exists
    cursor.execute(sql.SQL("DROP TABLE IF EXISTS {};").format(sql.Identifier(temp_table)))

    if join_multiple_areas:
        # run join
        if join_data_is_areas:
            if lad is True and gor is True and oa is False:
                cursor.execute(sql.SQL('SELECT b.{0} as gid, ARRAY_AGG(st_area(st_intersection(b.{1}, t.{2})) / st_area(b.{3})) as coverage, ARRAY_AGG(distinct t.geo_code) as lads, ARRAY_AGG(distinct t.geo_code_gor) as gors INTO {9} FROM ftables.{4} t, {5} b WHERE st_intersects(b.{6}, t.{7}) GROUP BY b.{8};').format(
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
            elif lad is True and gor is True and oa is True:
                cursor.execute(sql.SQL('SELECT b.{0} as gid, ARRAY_AGG(st_area(st_intersection(b.{1}, t.{2})) / st_area(b.{3})) as coverage, ARRAY_AGG(distinct t.oa_code) as oas, ARRAY_AGG(distinct t.geo_code) as lads, ARRAY_AGG(distinct t.geo_code_gor) as gors INTO {9} FROM ftables.{4} t, {5} b WHERE st_intersects(b.{6}, t.{7}) GROUP BY b.{8};').format(
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
                return 'Error! An intermediate step could not be completed. Please specify the spatial areas to be joined to the dataset.'

        else:
            #cursor.execute(sql.SQL('SELECT b.{0} as gid INTO {2} FROM {1} b;').format(sql.Identifier('gid'), sql.Identifier(dataset), sql.Identifier('_temp')))
            if lad is True and gor is True and oa is False:
                cursor.execute(sql.SQL('SELECT b.{0} as gid, ARRAY_AGG(distinct t.geo_code) as lads, ARRAY_AGG(distinct t.geo_code_gor) as gors INTO {5} FROM ftables.{1} t, {2} b WHERE st_intersects(b.{3}, t.{4} GROUP BY b.{6};').format(sql.Identifier(dataset_id_field), sql.Identifier(areas_to_join), sql.Identifier(dataset), sql.Identifier(dataset_geom_field), sql.Identifier(join_areas_geom_field), sql.Identifier(temp_table), sql.Identifier(dataset_id_field)))
            elif lad is True and gor is True and oa is True:
                cursor.execute(sql.SQL('SELECT b.{0} as gid, ARRAY_AGG(distinct t.oa_code) as oas, ARRAY_AGG(distinct t.geo_code) as lads, ARRAY_AGG(distinct t.geo_code_gor) as gors INTO {5} FROM ftables.{1} t, {2} b WHERE st_intersects(b.{3}, t.{4} GROUP BY b.{6};').format(sql.Identifier(dataset_id_field), sql.Identifier(areas_to_join), sql.Identifier(dataset), sql.Identifier(dataset_geom_field), sql.Identifier(join_areas_geom_field), sql.Identifier(temp_table), sql.Identifier(dataset_id_field)))
            else:
                return 'Error! An intermediate step could not be completed. Please specify the spatial areas to be joined to the dataset.'

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
        #cursor.execute(sql.SQL('DROP INDEX IF EXISTS {0};').format(sql.Identifier(temp_table+'_gid_idx')), [])
        #cursor.execute(sql.SQL('DROP TABLE IF EXISTS {0};').format(sql.Identifier(temp_table)))

        # create index on new fields which are now populated
        cursor.execute(sql.SQL('CREATE INDEX IF NOT EXISTS {0} ON {1} USING gin(lads);').format(sql.Identifier(dataset+'_lads_idx'), sql.Identifier(dataset)), [])
        cursor.execute(sql.SQL('CREATE INDEX IF NOT EXISTS {0} ON {1} USING gin(gors);').format(sql.Identifier(dataset+'_gor_idx'), sql.Identifier(dataset)), [])

    else:
        # if join result is to find a single area rather than multiple
        # create fields in dataset to join areas to
        # for field in fields_to_join:
        if oa:
            cursor.execute(sql.SQL('ALTER TABLE {} ADD IF NOT EXISTS oa character varying;').format(sql.Identifier(dataset)))
        if lad:
            cursor.execute(sql.SQL('ALTER TABLE {} ADD IF NOT EXISTS lad character varying;').format(sql.Identifier(dataset)))
        if gor:
            cursor.execute(sql.SQL('ALTER TABLE {} ADD IF NOT EXISTS gor character varying;').format(sql.Identifier(dataset)))

        # run spatial join
        if lad is True and gor is True and oa is False:
            cursor.execute(sql.SQL('UPDATE {0} a SET lad = b.lad_code, gor = b.gor_code FROM ftables.{1} b WHERE ST_Intersects(b.{2}, a.geom);').format(sql.Identifier(dataset), sql.Identifier(areas_to_join), sql.Identifier(dataset_geom_field)))
        elif lad is True and gor is True and oa is True:
            cursor.execute(sql.SQL('UPDATE {0} a SET oa = b.oa_code, lad = b.lad_code, gor = b.gor_code FROM ftables.{1} b WHERE ST_Intersects(b.{2}, a.geom);').format(sql.Identifier(dataset), sql.Identifier(areas_to_join), sql.Identifier(dataset_geom_field)))

    # delete temp table if it exists
    cursor.execute(sql.SQL("DROP TABLE IF EXISTS {};").format(sql.Identifier(temp_table)))

    # close connection an cursor
    cursor.close()
    database_connection.close()

    return True