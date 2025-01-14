"""
    show_catalogs(db)

Execute `SHOW CATALOGS` and return a vector of catalog names.
"""
function show_catalogs(db)
    sql = "SHOW CATALOGS"
    cr = DBInterface.execute(db, sql)
    Symbol.(Tables.columntable(cr)[1])
end

"""
    get_current_catalog(db)

Return the default catalog of the connection.
"""
function get_current_catalog(db)
    cr = DBInterface.execute(db, "SELECT current_catalog()")
    Symbol(Tables.columntable(cr)[1][1])
end

"""
    show_schemas(conn, catalog)

Execute `SHOW SCHEMAS` and return a vector of schema names.
"""
function show_schemas(db, catalog)
    catalog_sql = FunSQL.render(db, FunSQL.ID(catalog))
    sql = "SHOW SCHEMAS IN $catalog_sql"
    cr = DBInterface.execute(db, sql)
    Symbol.(Tables.columntable(cr)[1])
end

"""
    create_schema!(db, catalog, schema; replace = false, if_not_exists = false, comment = nothing)

Execute `CREATE SCHEMA`.
"""
function create_schema!(db, catalog, schema; replace = false, if_not_exists = false, comment = nothing)
    if replace
        drop_schema!(db, catalog, schema)
    end
    schema_sql = FunSQL.render(db, FunSQL.ID([catalog], schema))
    if_not_exists_sql = if_not_exists ? " IF NOT EXISTS" : ""
    comment_sql = comment !== nothing ? " COMMENT " * FunSQL.render(db, FunSQL.LIT(comment)) : ""
    sql = "CREATE SCHEMA$if_not_exists_sql $schema_sql$comment_sql"
    DBInterface.execute(db, sql)
    nothing
end

"""
    drop_schema!(db, catalog, schema)

Execute `DROP SCHEMA`.
"""
function drop_schema!(db, catalog, schema)
    schema_sql = FunSQL.render(db, FunSQL.ID([catalog], schema))
    sql = "DROP SCHEMA IF EXISTS $schema_sql CASCADE"
    DBInterface.execute(db, sql)
    nothing
end

"""
    show_volumes(db, catalog, schema)

Execute `SHOW VOLUMES` and return a vector of volume names.
"""
function show_volumes(db, catalog, schema)
    schema_sql = FunSQL.render(db, FunSQL.ID([catalog], schema))
    sql = "SHOW VOLUMES IN $schema_sql"
    cr = DBInterface.execute(db, sql)
    Symbol.(Tables.columntable(cr)[2])
end

"""
    create_volume!(db, catalog, schema, volume; replace = false, if_not_exists = false, comment = nothing)

Execute `CREATE VOLUME` and return the volume's base path.
"""
function create_volume!(db, catalog, schema, volume; replace = false, if_not_exists = false, comment = nothing)
    if replace
        drop_volume!(db; catalog, schema, volume)
    end
    volume_sql = FunSQL.render(db, FunSQL.ID([catalog, schema], volume))
    if_not_exists_sql = if_not_exists ? " IF NOT EXISTS" : ""
    comment_sql = comment !== nothing ? " COMMENT " * FunSQL.render(db, FunSQL.LIT(comment)) : ""
    sql = "CREATE VOLUME$if_not_exists_sql $volume_sql$comment_sql"
    DBInterface.execute(db, sql)
    joinpath("/Volumes", string(catalog), string(schema), string(volume))
end

"""
    drop_volume!(db, catalog, schema, volume; if_exists = true)

Execute `DROP VOLUME`.
"""
function drop_volume!(db, catalog, schema, volume)
    volume_sql = FunSQL.render(db, FunSQL.ID([catalog, schema], volume))
    sql = "DROP VOLUME IF EXISTS $volume_sql"
    DBInterface.execute(db, sql)
    nothing
end

"""
    show_files(db, remote_path)

Show files in a volume.
"""
function show_files(db, remote_path)
    remote_path_sql = FunSQL.render(db, FunSQL.LIT(remote_path))
    sql = "LIST $remote_path_sql"
    cr = DBInterface.execute(db, sql)
    Tables.columntable(cr)[1]
end

"""
    put_file!(db, local_path, remote_path; overwrite = false)

Upload a file to a volume.  The `local_path` must reside within `allowed_local_paths`
of the database connection.
"""
function put_file!(db, local_path, remote_path; overwrite = false)
    local_path_sql = FunSQL.render(db, FunSQL.LIT(abspath(local_path)))
    remote_path_sql = FunSQL.render(db, FunSQL.LIT(remote_path))
    overwrite_sql = overwrite ? " OVERWRITE" : ""
    sql = "PUT $local_path_sql INTO $remote_path_sql$overwrite_sql"
    DBInterface.execute(db, sql)
    nothing
end

"""
    get_file!(db, remote_path, local_path)

Download a file from a volume.  The `local_path` must reside within `allowed_local_paths`
of the database connection.
"""
function get_file!(db, remote_path, local_path)
    remote_path_sql = FunSQL.render(db, FunSQL.LIT(remote_path))
    local_path_sql = FunSQL.render(db, FunSQL.LIT(abspath(local_path)))
    sql = "GET $remote_path_sql TO $local_path_sql"
    DBInterface.execute(db, sql)
    nothing
end

"""
    remove_file!(db, remote_path)

Delete a file from a volume.
"""
function remove_file!(db, remote_path)
    remote_path_sql = FunSQL.render(db, FunSQL.LIT(remote_path))
    sql = "REMOVE $remote_path_sql"
    DBInterface.execute(db, sql)
    nothing
end

"""
    show_tables(db, catalog, schema)

Execute `SHOW TABLES` and return a vector of schema names.
"""
function show_tables(db, catalog, schema)
    schema_sql = FunSQL.render(db, FunSQL.ID([catalog], schema))
    sql = "SHOW TABLES IN $schema_sql"
    cr = DBInterface.execute(db, sql)
    Symbol.(Tables.columntable(cr)[2])
end

"""
    create_table!(db, catalog, schema, table, body; replace = false, if_not_exists = false, comment = nothing)

Execute `CREATE TABLE` and return a `SQLTable` object.
"""
function create_table!(db, catalog, schema, table, body; replace = false, if_not_exists = false, comment = nothing)
    table_sql = FunSQL.render(db, FunSQL.ID([catalog, schema], table))
    replace_sql = replace ? " OR REPLACE" : ""
    if_not_exists_sql = if_not_exists ? " IF NOT EXISTS" : ""
    comment_sql = comment !== nothing ? " COMMENT " * FunSQL.render(db, FunSQL.LIT(comment)) : ""
    body_sql = FunSQL.render(db, body)
    sql = "CREATE$replace_sql TABLE$if_not_exists_sql $table_sql$comment_sql AS\n$body_sql"
    DBInterface.execute(db, sql)
    FunSQL.SQLTable(qualifiers = [catalog, schema], table, columns = body_sql.columns)
end

"""
    create_table_from_file!(db, catalog, schema, table, path, opts...; replace = false, if_not_exists = false, comment = nothing)

Execute `CREATE TABLE ... FROM read_files(...)`.
"""
function create_table_from_file!(db, catalog, schema, table, path, opts...; replace = false, if_not_exists = false, comment = nothing)
    table_sql = FunSQL.render(db, FunSQL.ID([catalog, schema], table))
    replace_sql = replace ? " OR REPLACE" : ""
    if_not_exists_sql = if_not_exists ? " IF NOT EXISTS" : ""
    comment_sql = comment !== nothing ? " COMMENT " * FunSQL.render(db, FunSQL.LIT(comment)) : ""
    path_sql = FunSQL.render(db, FunSQL.LIT(path))
    opts_sql = join([", $k => " * FunSQL.render(db, FunSQL.LIT(v)) for (k, v) in opts])
    body_sql = "SELECT *\nFROM read_files($path_sql$opts_sql)"
    sql = "CREATE$replace_sql TABLE$if_not_exists_sql $table_sql$comment_sql AS\n$body_sql"
    DBInterface.execute(db, sql)
    sql = "SHOW COLUMNS IN $table_sql"
    cr = DBInterface.execute(db, sql)
    columns = Symbol.(Tables.columntable(cr)[1])
    FunSQL.SQLTable(qualifiers = [catalog, schema], table, columns = columns)
end

"""
    drop_table!(db, catalog, schema, table)

Execute `DROP TABLE`.
"""
function drop_table!(db, catalog, schema, table)
    table_sql = FunSQL.render(db, FunSQL.ID([catalog, schema], table))
    sql = "DROP TABLE IF EXISTS $table_sql"
    DBInterface.execute(db, sql)
    nothing
end
