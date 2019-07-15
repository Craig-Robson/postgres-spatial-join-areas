import psycopg2
from psycopg2 import sql


def create_database_connection(db_params):
    """
    Establish database connection to postgres
    """

    connection = psycopg2.connect("dbname=%s user=%s port=%s host=%s password=%s" %(db_params['database_name'], db_params['user'], db_params['port'], db_params['host'], db_params['password']))

    connection.autocommit = True

    return connection


def main(connection=None, connection_parameters=None, dataset='', dataset_area=True, areas_to_join='', fields_to_join=[]):
    """
    Run the spatial join over two tables existing in a postgres database.

    fields_to_join:
     - [{name:xxx, type:xxx},{name:yyy, type:yyy}]
    """
    if connection is None:
        if connection_parameters is None:
            # return an error to the user
            return
        db_connection = create_database_connection(connection_parameters)

    # create cursor to access database
    cursor = db_connection.cursor()

    temp_table = '___temp'

    # run join
    if dataset_area:
        cursor.execute(sql.SQL('SELECT b.gid, ARRAY_AGG(st_area(st_intersection(b.geom, t.geom)) / st_area(b.geom)) as coverage, ARRAY_AGG(t.geo_code) as lads, ARRAY_AGG(t.geo_code_gor) as gors INTO {} FROM ftables.{} t, {} b WHERE st_intersects(b.geom, t.geom) ORDER BY g.gid;').format(sql.Identifier(temp_table), sql.Identifier(areas_to_join), sql.Identifier(dataset))) #'-- and (st_area(st_intersection(b.geom, t.geom)) / st_area(b.geom)) > .10
    else:
        cursor.execute(sql.SQL('SELECT b.gid, ARRAY_AGG(t.geo_code) as lads, ARRAY_AGG(t.geo_code_gor) as gors INTO ___temp FROM ftables.{} t, {} b WHERE st_intersects(b.geom_, t.geom) GROUP BY b.gid;').format(sql.Identifier(areas_to_join), sql.Identifier(dataset)))

    # create fields in dataset to join areas to
    #for field in fields_to_join:
    cursor.execute(sql.SQL('ALTER TABLE {} ADD IF NOT EXISTS lads character varying[];').format(sql.Identifier(dataset)))
    cursor.execute(sql.SQL('ALTER TABLE {} ADD IF NOT EXISTS gors character varying[];').format(sql.Identifier(dataset)))

    # add field for coverage values if join involves areas
    if dataset_area:
        cursor.execute(sql.SQL('ALTER TABLE {} ADD IF NOT EXISTS lad_coverage float[];').format(sql.Identifier(dataset)))

    # create index on the ids to speed up the update
    cursor.execute(sql.SQL('CREATE INDEX ___temp_gid_idx ON {} USING btree(gid);').format(sql.Identifier(temp_table)))
    cursor.execute(sql.SQL('CREATE INDEX IF NOT EXISTS scheduledmonuments2019_gid_idx ON {} USING btree(gid);').format(sql.Identifier(dataset)))

    # run the update
    cursor.execute(sql.SQL('UPDATE {} as a SET lads = b.lads, gors = b.gors, lad_coverage = b.coverage FROM {} b WHERE a.gid = b.gid;').format(sql.Identifier(dataset), sql.Identifier(temp_table)))

    # remote the temp tables
    cursor.execute(sql.SQL('DROP INDEX ___temp_gid_idx;').format(sql.Identifier()))
    cursor.execute(sql.SQL('DROP TABLE {};').format(sql.Identifier(temp_table)))

    # create index on new fields which are now populated
    cursor.execute(sql.SQL('CREATE INDEX IF NOT EXISTS %s ON {} USING gin(lads);').format(sql.Identifier(dataset)), [dataset+'_lads_idx'])
    cursor.execute(sql.SQL('CREATE INDEX IF NOT EXISTS %s ON {} USING gin(gors);').format(sql.Identifier(dataset)), [dataset+'_gor_idx'])
