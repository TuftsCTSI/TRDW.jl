module TRDW

export @run_funsql

using DataFrames
using ODBC
using FunSQL
using Dates
using CSV
using ZipFile

const wide_notebook_style = html"""
<style>
    @media screen and (min-width: calc(700px + 25px + 283px + 34px + 25px)) {
        main {
            margin: 0 auto;
            max-width: 2000px;
            padding-right: calc(283px + 34px + 25px);
        }
    }
</style>
"""

WideStyle() =
    wide_notebook_style

build_dsn(; kws...) =
    join(["$key=$val" for (key, val) in pairs(kws)], ';')

function connect_to_databricks(; catalog = nothing, schema = nothing)
    DATABRICKS_SERVER_HOSTNAME = ENV["DATABRICKS_SERVER_HOSTNAME"]
    DATABRICKS_HTTP_PATH = ENV["DATABRICKS_HTTP_PATH"]
    DATABRICKS_ACCESS_TOKEN = ENV["DATABRICKS_ACCESS_TOKEN"]
    DATABRICKS_CATALOG = ENV["DATABRICKS_CATALOG"]

    catalog = something(catalog, DATABRICKS_CATALOG)
    schema = get(ENV, "TRDW_SCHEMA", schema)

    DATABRICKS_DSN = build_dsn(
        Driver = "/opt/simba/spark/lib/64/libsparkodbc_sb64.so",
        Host = DATABRICKS_SERVER_HOSTNAME,
        Port = 443,
        SSL = 1,
        ThriftTransport = 2,
        HTTPPath = DATABRICKS_HTTP_PATH,
        UseNativeQuery = 1,
        AuthMech = 3,
        Catalog = catalog,
        Schema = schema,
        UID = "token",
        PWD = DATABRICKS_ACCESS_TOKEN)

    DBInterface.connect(ODBC.Connection, DATABRICKS_DSN)
end

function connect_with_funsql(conn::ODBC.Connection, cols)
    cat = FunSQL.SQLCatalog(
        tables = FunSQL.tables_from_column_list(cols),
        dialect = FunSQL.SQLDialect(:spark))

    FunSQL.SQLConnection(conn, catalog = cat)
end

function connect_with_funsql(specs...; catalog = nothing, exclude = nothing)
    DATABRICKS_CATALOG = ENV["DATABRICKS_CATALOG"]
    catalog = something(catalog, DATABRICKS_CATALOG)

    conn = connect_to_databricks(catalog = catalog)
    table_map = Dict{Symbol, FunSQL.SQLTable}()
    for spec in specs
        prefix, schemaname = _unpack_spec(spec)
        raw_cols = ODBC.columns(conn,
                                catalogname = catalog,
                                schemaname = schemaname)
        cols = [(lowercase(row.TABLE_SCHEM),
                 lowercase(row.TABLE_NAME),
                 lowercase(row.COLUMN_NAME))
                for row in Tables.rows(raw_cols)]
        tables = FunSQL.tables_from_column_list(cols)
        for table in tables
            exclude === nothing || !exclude(table) || continue
            name = Symbol("$prefix$(table.name)")
            table_map[name] = table
        end
    end
    cat = FunSQL.SQLCatalog(tables = table_map, dialect = FunSQL.SQLDialect(:spark))

    FunSQL.SQLConnection(conn, catalog = cat)
end

_unpack_spec(schema::Union{Symbol, AbstractString}) =
    "", string(schema)

_unpack_spec(pair::Pair) =
    string(first(pair)), string(last(pair))

function cursor_to_dataframe(cr)
    df = DataFrame(cr)
    # Remove `Missing` from column types where possible.
    disallowmissing!(df, error = false)
    df
end

run(db, q) =
    DBInterface.execute(db, q) |>
    cursor_to_dataframe

macro run_funsql(db, q)
    :(run($db, @funsql($q)))
end

function describe_all(db)
    tables = Pair{Symbol, Any}[]
    for name in sort(collect(keys(db.catalog)))
        fields = describe_table(db, name)
        push!(tables, name => fields)
    end
    Dict(tables)
end

function describe_table(db, name)
    t = db.catalog[name]
    ddl = run(db, "SHOW CREATE TABLE `$(t.schema)`.`$(t.name)`")[1,1]
    cols = match(r"\(\s*([^)]+)\)", ddl)[1]
    toks = split(cols, r"\s+|(?=\W)|\b")
    i = 1
    fields = Pair{Symbol, Any}[]
    done = false
    while !done
        f, i = parse_name(toks, i)
        ft, i = parse_type(toks, i)
        if i <= length(toks)
            _, i = parse_punctuation([","], toks, i)
        else
            done = true
        end
        push!(fields, f => ft)
    end
    fields
end

function parse_name(toks, i)
    i <= length(toks) || error("unexpected end of type string")
    tok = toks[i]
    i += 1
    occursin(r"\A\w+\z", tok) || error("unexpected token $tok")
    return Symbol(tok), i
end

function parse_type(toks, i)
    i <= length(toks) || error("unexpected end of type string")
    tok = toks[i]
    i += 1
    if tok in ["BOOLEAN", "SMALLINT", "INT", "FLOAT", "STRING", "DATE", "TIMESTAMP", "BINARY"]
        not_null, i = parse_not_null(toks, i)
        return (type = Symbol(lowercase(tok)), not_null = not_null), i
    elseif tok == "ARRAY"
        _, i = parse_punctuation(["<"], toks, i)
        elt, i = parse_type(toks, i)
        _, i = parse_punctuation([">"], toks, i)
        not_null, i = parse_not_null(toks, i)
        return (type = :array, eltype = elt, not_null = not_null), i
    elseif tok == "STRUCT"
        _, i = parse_punctuation(["<"], toks, i)
        fields = Pair{Symbol, Any}[]
        done = false
        while !done
            f, i = parse_name(toks, i)
            _, i = parse_punctuation([":"], toks, i)
            ft, i = parse_type(toks, i)
            push!(fields, f => ft)
            tok, i = parse_punctuation([",", ">"], toks, i)
            done = tok == ">"
        end
        not_null, i = parse_not_null(toks, i)
        return (type = :struct, fields = fields, not_null = not_null), i
    else
        error("unexpected token $tok")
    end
end

function parse_not_null(toks, i)
    if i + 1 <= length(toks) && toks[i] == "NOT" && toks[i + 1] == "NULL"
        return true, i + 2
    else
        return false, i
    end
end

function parse_punctuation(vals, toks, i)
    i <= length(toks) || error("unexpected end of type string")
    tok = toks[i]
    i += 1
    tok in vals || error("unexpected token $tok")
    return tok, i
end

@funsql restrict_by(q) =
    restrict_by(person_id, $q)

@funsql restrict_by(column_name, q) =
    join(
        subset => $q.filter(is_not_null($column_name)).group($column_name),
        $column_name == subset.$column_name)

function temp_table!(etl, name, def)
    schema = etl.db.catalog[:person].schema
    ref = Ref{Pair{FunSQL.SQLTable, FunSQL.SQLClause}}()
    q = FunSQL.From(name) |> FunSQL.WithExternal(name => def, schema = schema, handler = (p -> ref[] = p))
    FunSQL.render(etl.db, q)
    t, c = ref[]
    name_sql = FunSQL.render(etl.db, FunSQL.ID(t.schema) |> FunSQL.ID(t.name))
    sql = FunSQL.render(etl.db, c)
    create_stmt = "CREATE TABLE $name_sql AS\n$sql"
    drop_stmt = "DROP TABLE $name_sql"
    push!(etl.create_stmts, create_stmt)
    push!(etl.drop_stmts, drop_stmt)
    return FunSQL.From(t)
end

function zipfile(filename, db, pairs...)
    z = ZipFile.Writer(filename)
    for (name, q) in pairs
        f = ZipFile.addfile(z, name)
        cr = DBInterface.execute(db, q)
        CSV.write(f, cr)
    end
    close(z)
end

function export_zip(filename, db, input_q)
    etl = (db = db, create_stmts = String[], drop_stmts = String[])
    suffix = Dates.format(Dates.now(), "yyyymmddHHMMSSZ")
    cohort_q =
        temp_table!(
            etl,
            "cohort_$suffix",
            @funsql $input_q.filter(is_not_null(person_id)).group(person_id))
    person_q =
        temp_table!(
            etl,
            "person_$suffix",
            @funsql from(person).restrict_by($cohort_q))
    observation_period_q =
        temp_table!(
            etl,
            "observation_period_$suffix",
            @funsql from(observation_period).restrict_by($cohort_q))
    visit_occurrence_q =
        temp_table!(
            etl,
            "visit_occurrence_$suffix",
            @funsql from(visit_occurrence).restrict_by($cohort_q))
    visit_detail_q =
        temp_table!(
            etl,
            "visit_detail_$suffix",
            @funsql from(visit_detail).restrict_by($cohort_q))
    condition_occurrence_q =
        temp_table!(
            etl,
            "condition_occurrence_$suffix",
            @funsql from(condition_occurrence).restrict_by($cohort_q))
    drug_exposure_q =
        temp_table!(
            etl,
            "drug_exposure_$suffix",
            @funsql from(drug_exposure).restrict_by($cohort_q))
    procedure_occurrence_q =
        temp_table!(
            etl,
            "procedure_occurrence_$suffix",
            @funsql from(procedure_occurrence).restrict_by($cohort_q))
    device_exposure_q =
        temp_table!(
            etl,
            "device_exposure_$suffix",
            @funsql from(device_exposure).restrict_by($cohort_q))
    measurement_q =
        temp_table!(
            etl,
            "measurement_$suffix",
            @funsql from(measurement).restrict_by($cohort_q))
    observation_q =
        temp_table!(
            etl,
            "observation_$suffix",
            @funsql from(observation).restrict_by($cohort_q))
    death_q =
        temp_table!(
            etl,
            "death_$suffix",
            @funsql from(death).restrict_by($cohort_q))
    note_q =
        temp_table!(
            etl,
            "note_$suffix",
            @funsql from(note).restrict_by($cohort_q))
    note_nlp_q =
        temp_table!(
            etl,
            "note_nlp_$suffix",
            @funsql from(note_nlp).restrict_by(note_id, $note_q))
    specimen_q =
        temp_table!(
            etl,
            "specimen_$suffix",
            @funsql from(specimen).restrict_by($cohort_q))
    provider_q =
        temp_table!(
            etl,
            "provider_$suffix",
            @funsql begin
                from(provider)
                restrict_by(
                    provider_id,
                    append(
                        $person_q,
                        $visit_occurrence_q,
                        $visit_detail_q,
                        $condition_occurrence_q,
                        $drug_exposure_q,
                        $procedure_occurrence_q,
                        $device_exposure_q,
                        $measurement_q,
                        $observation_q,
                        $note_q))
            end)
    care_site_q =
        temp_table!(
            etl,
            "care_site_$suffix",
            @funsql begin
                from(care_site)
                restrict_by(
                    care_site_id,
                    append(
                        $person_q,
                        $visit_occurrence_q,
                        $visit_detail_q,
                        $provider_q))
            end)
    location_q =
        temp_table!(
            etl,
            "location_$suffix",
            @funsql begin
                from(location)
                restrict_by(
                    location_id,
                    append(
                        $person_q,
                        $care_site_q))
            end)
    fact_q =
        temp_table!(
            etl,
            "fact_$suffix",
            @funsql begin
                append(
                    $person_q.define(fact_id => person_id, domain_concept_id => 1147314),
                    $observation_period_q.define(fact_id => observation_period_id, domain_concept_id => 1147321),
                    $visit_occurrence_q.define(fact_id => visit_occurrence_id, domain_concept_id => 1147332),
                    $visit_detail_q.define(fact_id => visit_detail_id, domain_concept_id => 1147637),
                    $condition_occurrence_q.define(fact_id => condition_occurrence_id, domain_concept_id => 1147333),
                    $drug_exposure_q.define(fact_id => drug_exposure_id, domain_concept_id => 1147339),
                    $procedure_occurrence_q.define(fact_id => procedure_occurrence_id, domain_concept_id => 1147301),
                    $device_exposure_q.define(fact_id => device_exposure_id, domain_concept_id => 1147305),
                    $measurement_q.define(fact_id => measurement_id, domain_concept_id => 1147330),
                    $observation_q.define(fact_id => observation_id, domain_concept_id => 1147304),
                    $note_q.define(fact_id => note_id, domain_concept_id => 1147317),
                    $note_nlp_q.define(fact_id => note_nlp_id, domain_concept_id => 1147542),
                    $specimen_q.define(fact_id => specimen_id, domain_concept_id => 1147306),
                    $provider_q.define(fact_id => provider_id, domain_concept_id => 1147315),
                    $care_site_q.define(fact_id => care_site_id, domain_concept_id => 1147313),
                    $location_q.define(fact_id => location_id, domain_concept_id => 1147335))
                group(fact_id, domain_concept_id)
            end)
    fact_relationship_q =
        temp_table!(
            etl,
            "fact_relationship_$suffix",
            @funsql begin
                from(fact_relationship)
                join(
                    subset_1 => $fact_q,
                    domain_concept_id_1 == subset_1.domain_concept_id && fact_id_1 == subset_1.fact_id)
                join(
                    subset_2 => $fact_q,
                    domain_concept_id_2 == subset_2.domain_concept_id && fact_id_2 == subset_2.fact_id)
            end)
    payer_plan_period_q =
        temp_table!(
            etl,
            "payer_plan_period_$suffix",
            @funsql from(payer_plan_period).filter(false))
    cost_q =
        temp_table!(
            etl,
            "cost_$suffix",
            @funsql from(cost).filter(false))
    drug_era_q =
        temp_table!(
            etl,
            "drug_era_$suffix",
            @funsql from(drug_era).restrict_by($cohort_q))
    dose_era_q =
        temp_table!(
            etl,
            "dose_era_$suffix",
            @funsql from(dose_era).restrict_by($cohort_q))
    condition_era_q =
        temp_table!(
            etl,
            "condition_era_$suffix",
            @funsql from(condition_era).restrict_by($cohort_q))
    episode_q =
        temp_table!(
            etl,
            "episode_$suffix",
            @funsql from(episode).restrict_by($cohort_q))
    episode_event_q =
        temp_table!(
            etl,
            "episode_event_$suffix",
            @funsql from(episode_event).restrict_by(episode_id, $episode_q))
    metadata_q =
        temp_table!(
            etl,
            "metadata_$suffix",
            @funsql from(metadata))
    cdm_source_q =
        temp_table!(
            etl,
            "cdm_source_$suffix",
            @funsql from(cdm_source))
    vocabulary_q =
        temp_table!(
            etl,
            "vocabulary_$suffix",
            @funsql from(vocabulary))
    domain_q =
        temp_table!(
            etl,
            "domain_$suffix",
            @funsql from(domain))
    concept_class_q =
        temp_table!(
            etl,
            "concept_class_$suffix",
            @funsql from(concept_class))
    relationship_q =
        temp_table!(
            etl,
            "relationship_$suffix",
            @funsql from(relationship))
    drug_strength_q =
        temp_table!(
            etl,
            "drug_strength_$suffix",
            @funsql from(drug_strength).restrict_by(drug_concept_id, $drug_exposure_q.group(drug_concept_id)))
    concept_q =
        temp_table!(
            etl,
            "concept_$suffix",
            @funsql begin
                from(concept)
                restrict_by(
                    concept_id,
                    append(
                        $person_q.define(concept_id => gender_concept_id),
                        $person_q.define(concept_id => race_concept_id),
                        $person_q.define(concept_id => ethnicity_concept_id),
                        $person_q.define(concept_id => gender_source_concept_id),
                        $person_q.define(concept_id => race_source_concept_id),
                        $person_q.define(concept_id => ethnicity_source_concept_id),
                        $observation_period_q.define(concept_id => period_type_concept_id),
                        $visit_occurrence_q.define(concept_id => visit_concept_id),
                        $visit_occurrence_q.define(concept_id => visit_type_concept_id),
                        $visit_occurrence_q.define(concept_id => visit_source_concept_id),
                        $visit_occurrence_q.define(concept_id => admitted_from_concept_id),
                        $visit_occurrence_q.define(concept_id => discharged_to_concept_id),
                        $visit_detail_q.define(concept_id => visit_detail_concept_id),
                        $visit_detail_q.define(concept_id => visit_detail_type_concept_id),
                        $visit_detail_q.define(concept_id => visit_detail_source_concept_id),
                        $visit_detail_q.define(concept_id => admitted_from_concept_id),
                        $visit_detail_q.define(concept_id => discharged_to_concept_id),
                        $condition_occurrence_q.define(concept_id => condition_concept_id),
                        $condition_occurrence_q.define(concept_id => condition_type_concept_id),
                        $condition_occurrence_q.define(concept_id => condition_status_concept_id),
                        $condition_occurrence_q.define(concept_id => condition_source_concept_id),
                        $drug_exposure_q.define(concept_id => drug_concept_id),
                        $drug_exposure_q.define(concept_id => drug_type_concept_id),
                        $drug_exposure_q.define(concept_id => route_concept_id),
                        $drug_exposure_q.define(concept_id => drug_source_concept_id),
                        $procedure_occurrence_q.define(concept_id => procedure_concept_id),
                        $procedure_occurrence_q.define(concept_id => procedure_type_concept_id),
                        $procedure_occurrence_q.define(concept_id => modifier_concept_id),
                        $procedure_occurrence_q.define(concept_id => procedure_source_concept_id),
                        $device_exposure_q.define(concept_id => device_concept_id),
                        $device_exposure_q.define(concept_id => device_type_concept_id),
                        $device_exposure_q.define(concept_id => device_source_concept_id),
                        $device_exposure_q.define(concept_id => unit_concept_id),
                        $device_exposure_q.define(concept_id => unit_source_concept_id),
                        $measurement_q.define(concept_id => measurement_concept_id),
                        $measurement_q.define(concept_id => measurement_type_concept_id),
                        $measurement_q.define(concept_id => operator_concept_id),
                        $measurement_q.define(concept_id => value_as_concept_id),
                        $measurement_q.define(concept_id => unit_concept_id),
                        $measurement_q.define(concept_id => measurement_source_concept_id),
                        $measurement_q.define(concept_id => unit_source_concept_id),
                        $measurement_q.define(concept_id => meas_event_field_concept_id),
                        $observation_q.define(concept_id => observation_concept_id),
                        $observation_q.define(concept_id => observation_type_concept_id),
                        $observation_q.define(concept_id => value_as_concept_id),
                        $observation_q.define(concept_id => qualifier_concept_id),
                        $observation_q.define(concept_id => unit_concept_id),
                        $observation_q.define(concept_id => observation_source_concept_id),
                        $observation_q.define(concept_id => obs_event_field_concept_id),
                        $death_q.define(concept_id => death_type_concept_id),
                        $death_q.define(concept_id => cause_concept_id),
                        $death_q.define(concept_id => cause_source_concept_id),
                        $note_q.define(concept_id => note_type_concept_id),
                        $note_q.define(concept_id => note_class_concept_id),
                        $note_q.define(concept_id => encoding_concept_id),
                        $note_q.define(concept_id => language_concept_id),
                        $note_q.define(concept_id => note_event_field_concept_id),
                        $note_nlp_q.define(concept_id => section_concept_id),
                        $note_nlp_q.define(concept_id => note_nlp_concept_id),
                        $note_nlp_q.define(concept_id => note_nlp_source_concept_id),
                        $specimen_q.define(concept_id => specimen_concept_id),
                        $specimen_q.define(concept_id => specimen_type_concept_id),
                        $specimen_q.define(concept_id => unit_concept_id),
                        $specimen_q.define(concept_id => anatomic_site_concept_id),
                        $provider_q.define(concept_id => specialty_concept_id),
                        $provider_q.define(concept_id => gender_concept_id),
                        $provider_q.define(concept_id => specialty_source_concept_id),
                        $provider_q.define(concept_id => gender_source_concept_id),
                        $care_site_q.define(concept_id => place_of_service_concept_id),
                        $location_q.define(concept_id => country_concept_id),
                        $fact_relationship_q.define(concept_id => domain_concept_id_1),
                        $fact_relationship_q.define(concept_id => domain_concept_id_2),
                        $fact_relationship_q.define(concept_id => relationship_concept_id),
                        $payer_plan_period_q.define(concept_id => payer_concept_id),
                        $payer_plan_period_q.define(concept_id => payer_source_concept_id),
                        $payer_plan_period_q.define(concept_id => plan_concept_id),
                        $payer_plan_period_q.define(concept_id => plan_source_concept_id),
                        $payer_plan_period_q.define(concept_id => sponsor_concept_id),
                        $payer_plan_period_q.define(concept_id => sponsor_source_concept_id),
                        $payer_plan_period_q.define(concept_id => stop_reason_concept_id),
                        $payer_plan_period_q.define(concept_id => stop_reason_source_concept_id),
                        $cost_q.define(concept_id => cost_type_concept_id),
                        $cost_q.define(concept_id => currency_concept_id),
                        $cost_q.define(concept_id => revenue_code_concept_id),
                        $cost_q.define(concept_id => drg_concept_id),
                        $drug_era_q.define(concept_id => drug_concept_id),
                        $dose_era_q.define(concept_id => drug_concept_id),
                        $dose_era_q.define(concept_id => unit_concept_id),
                        $condition_era_q.define(concept_id => condition_concept_id),
                        $episode_q.define(concept_id => episode_object_concept_id),
                        $episode_q.define(concept_id => episode_type_concept_id),
                        $episode_q.define(concept_id => episode_source_concept_id),
                        $episode_event_q.define(concept_id => episode_event_field_concept_id),
                        $metadata_q.define(concept_id => metadata_concept_id),
                        $metadata_q.define(concept_id => metadata_type_concept_id),
                        $metadata_q.define(concept_id => value_as_concept_id),
                        $cdm_source_q.define(concept_id => cdm_version_concept_id),
                        $vocabulary_q.define(concept_id => vocabulary_concept_id),
                        $domain_q.define(concept_id => domain_concept_id),
                        $concept_class_q.define(concept_id => concept_class_concept_id),
                        $relationship_q.define(concept_id => relationship_concept_id),
                        $drug_strength_q.define(concept_id => drug_concept_id),
                        $drug_strength_q.define(concept_id => ingredient_concept_id),
                        $drug_strength_q.define(concept_id => amount_unit_concept_id),
                        $drug_strength_q.define(concept_id => numerator_unit_concept_id),
                        $drug_strength_q.define(concept_id => denominator_unit_concept_id),
                        from(concept_synonym).define(concept_id => language_concept_id)))
            end)
    concept_synonym_q =
        temp_table!(
            etl,
            "concept_synonym_$suffix",
            @funsql from(concept_synonym).restrict_by(concept_id, $concept_q))
    concept_relationship_q =
        temp_table!(
            etl,
            "concept_relationship_$suffix",
            @funsql begin
                from(concept_relationship)
                join(
                    subset_1 => $concept_q,
                    concept_id_1 == subset_1.concept_id)
                join(
                    subset_2 => $concept_q,
                    concept_id_2 == subset_2.concept_id)
            end)
    concept_ancestor_q =
        temp_table!(
            etl,
            "concept_ancestor_$suffix",
            @funsql begin
                from(concept_ancestor)
                join(
                    subset_1 => $concept_q,
                    ancestor_concept_id == subset_1.concept_id)
                join(
                    subset_2 => $concept_q,
                    descendant_concept_id == subset_2.concept_id)
            end)
    for stmt in etl.create_stmts
        println(stmt)
        DBInterface.execute(db, stmt)
    end
    zipfile(
        filename,
        db,
        "person.csv" => person_q,
        "observation_period.csv" => observation_period_q,
        "visit_occurrence.csv" => visit_occurrence_q,
        "visit_detail.csv" => visit_detail_q,
        "condition_occurrence.csv" => condition_occurrence_q,
        "drug_exposure.csv" => drug_exposure_q,
        "procedure_occurrence.csv" => procedure_occurrence_q,
        "device_exposure.csv" => device_exposure_q,
        "measurement.csv" => measurement_q,
        "observation.csv" => observation_q,
        "death.csv" => death_q,
        "note.csv" => note_q,
        "note_nlp.csv" => note_nlp_q,
        "specimen.csv" => specimen_q,
        "provider.csv" => provider_q,
        "care_site.csv" => care_site_q,
        "location.csv" => location_q,
        "fact_relationship.csv" => fact_relationship_q,
        "payer_plan_period.csv" => payer_plan_period_q,
        "cost.csv" => cost_q,
        "drug_era.csv" => drug_era_q,
        "dose_era.csv" => dose_era_q,
        "condition_era.csv" => condition_era_q,
        "episode.csv" => episode_q,
        "episode_event.csv" => episode_event_q,
        "metadata.csv" => metadata_q,
        "cdm_source.csv" => cdm_source_q,
        "vocabulary.csv" => vocabulary_q,
        "domain.csv" => domain_q,
        "concept_class.csv" => concept_class_q,
        "relationship.csv" => relationship_q,
        "drug_strength.csv" => drug_strength_q,
        "concept.csv" => concept_q,
        "concept_synonym.csv" => concept_synonym_q,
        "concept_relationship.csv" => concept_relationship_q,
        "concept_ancestor.csv" => concept_ancestor_q)
    for stmt in etl.drop_stmts
        println(stmt)
        DBInterface.execute(db, stmt)
    end
end

end
