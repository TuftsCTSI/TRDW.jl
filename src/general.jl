env_catalog() = Symbol(get(ENV, "DATABRICKS_CATALOG", "ctsi"))

sqlname(db, schema::Symbol) =
    FunSQL.render(db, FunSQL.ID(env_catalog()) |> FunSQL.ID(schema))

sqlname(db, schema::Symbol, table::Symbol) =
    FunSQL.render(db, FunSQL.ID(env_catalog()) |> FunSQL.ID(schema) |> FunSQL.ID(table))

sqlname(db, t::FunSQL.SQLTable) =
    FunSQL.render(db, FunSQL.ID(t.qualifiers, t.name))

sqlname(db, node::FunSQL.SQLNode) =
    sqlname(db, getfield(getfield(node, :core), :source))

function create_table_if_not_exists(db, schema::Symbol, table::Symbol, spec...)
    schema_name_sql = sqlname(db, schema)
    name_sql = sqlname(db, schema, table)
    cols = [p[1] for p in spec]
    spec = join(["$(string(p[1])) $(string(p[2]))" for p in spec], ", ")
    DBInterface.execute(db, "CREATE SCHEMA IF NOT EXISTS $(schema_name_sql)")
    DBInterface.execute(db, "GRANT ALL PRIVILEGES ON SCHEMA $(schema_name_sql) to CTSIStaff")
    DBInterface.execute(db, "CREATE TABLE IF NOT EXISTS $(name_sql) ($spec)")
    DBInterface.execute(db, "GRANT ALL PRIVILEGES ON TABLE $(name_sql) to CTSIStaff")
    return FunSQL.SQLTable(qualifiers = [env_catalog(), schema], name = table, columns = cols)
end

struct CreateTableSpecification
    schema_name::Symbol
    name::Symbol
    node::FunSQL.SQLNode
end

"""
@query write_table(table_name => table_content(); schema=user_schema())

Create *or replace* the named table using contents from the query.
This returns a `FunSQL.SQLTable` object that would need to be wrapped in a `FunSQL.FromNode` to be used.
This creates the schema if it is not exists and assigns appropriate permissions.

Example usage
-------------

```
begin
    t = @query write_table(cohort => cohort_definition())
    @funsql cohort() = from(\$t)
end
```

"""
funsql_write_table((name, node)::Pair{<:Union{Symbol, AbstractString}, <:Any};
                    schema::Union{Symbol, AbstractString} = funsql_user_schema()) =
    CreateTableSpecification(Symbol(schema), Symbol(name), node)

function run(db, spec::CreateTableSpecification)
    schema_name_sql = FunSQL.render(db, FunSQL.ID(spec.schema_name))
    name_sql = FunSQL.render(db, FunSQL.ID([spec.schema_name], spec.name))
    sql = FunSQL.render(db, spec.node)
    if false
        # TODO: requires FunSQL#metadata branch
        # t = FunSQL.SQLTable(qualifiers = [spec.schema_name], spec.name, columns = sql.columns)
    else
        ref = Ref{Pair{FunSQL.SQLTable, FunSQL.SQLClause}}()
        q = FunSQL.From(spec.name) |> FunSQL.WithExternal(spec.name => spec.node,
                                                        qualifiers = [spec.schema_name],
                                                        handler = (p -> ref[] = p))
        FunSQL.render(db, q)
        t, c = ref[]
    end
    DBInterface.execute(db, "CREATE SCHEMA IF NOT EXISTS $(schema_name_sql)")
    DBInterface.execute(db, "GRANT ALL PRIVILEGES ON SCHEMA $(schema_name_sql) to CTSIStaff")
    DBInterface.execute(db, "CREATE OR REPLACE TABLE $(name_sql) AS\n$sql")
    DBInterface.execute(db, "GRANT ALL PRIVILEGES ON TABLE $(name_sql) to CTSIStaff")
    DBInterface.execute(db, "COMMENT ON TABLE $(name_sql) IS '$(Dates.now())'")
    return t
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
    name = FunSQL.render(db, FunSQL.ID(t.qualifiers, t.name))
    ddl = run(db, "SHOW CREATE TABLE $name")[1,1]
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

clarity_dict(table_name) =
    HTML("""<a href="https://datahandbook.epic.com/ClarityDictionary/Details?tblName=$table_name"><code>$table_name</code></a>""")

fhir(table) =
    HTML("""<a href="https://www.hl7.org/fhir/$(lowercase(string(table))).html"><code>$(string(table))</code></a>""")

const omop_catalog = FunSQL.SQLCatalog(
    FunSQL.SQLTable(
        :person,
        :person_id,
        :gender_concept_id,
        :year_of_birth,
        :month_of_birth,
        :day_of_birth,
        :birth_datetime,
        :race_concept_id,
        :ethnicity_concept_id,
        :location_id,
        :provider_id,
        :care_site_id,
        :person_source_value,
        :gender_source_value,
        :gender_source_concept_id,
        :race_source_value,
        :race_source_concept_id,
        :ethnicity_source_value,
        :ethnicity_source_concept_id),
    FunSQL.SQLTable(
        :observation_period,
        :observation_period_id,
        :person_id,
        :observation_period_start_date,
        :observation_period_end_date,
        :period_type_concept_id),
    FunSQL.SQLTable(
        :visit_occurrence,
        :visit_occurrence_id,
        :person_id,
        :visit_concept_id,
        :visit_start_date,
        :visit_start_datetime,
        :visit_end_date,
        :visit_end_datetime,
        :visit_type_concept_id,
        :provider_id,
        :care_site_id,
        :visit_source_value,
        :visit_source_concept_id,
        :admitted_from_concept_id,
        :admitted_from_source_value,
        :discharged_to_concept_id,
        :discharged_to_source_value,
        :preceding_visit_occurrence_id),
    FunSQL.SQLTable(
        :visit_detail,
        :visit_detail_id,
        :person_id,
        :visit_detail_concept_id,
        :visit_detail_start_date,
        :visit_detail_start_datetime,
        :visit_detail_end_date,
        :visit_detail_end_datetime,
        :visit_detail_type_concept_id,
        :provider_id,
        :care_site_id,
        :visit_detail_source_value,
        :visit_detail_source_concept_id,
        :admitted_from_concept_id,
        :admitted_from_source_value,
        :discharged_to_concept_id,
        :discharged_to_source_value,
        :preceding_visit_detail_id,
        :parent_visit_detail_id,
        :visit_occurrence_id),
    FunSQL.SQLTable(
        :condition_occurrence,
        :condition_occurrence_id,
        :person_id,
        :condition_concept_id,
        :condition_start_date,
        :condition_start_datetime,
        :condition_end_date,
        :condition_end_datetime,
        :condition_type_concept_id,
        :condition_status_concept_id,
        :stop_reason,
        :provider_id,
        :visit_occurrence_id,
        :visit_detail_id,
        :condition_source_value,
        :condition_source_concept_id,
        :condition_status_source_value),
    FunSQL.SQLTable(
        :drug_exposure,
        :drug_exposure_id,
        :person_id,
        :drug_concept_id,
        :drug_exposure_start_date,
        :drug_exposure_start_datetime,
        :drug_exposure_end_date,
        :drug_exposure_end_datetime,
        :verbatim_end_date,
        :drug_type_concept_id,
        :stop_reason,
        :refills,
        :quantity,
        :days_supply,
        :sig,
        :route_concept_id,
        :lot_number,
        :provider_id,
        :visit_occurrence_id,
        :visit_detail_id,
        :drug_source_value,
        :drug_source_concept_id,
        :route_source_value,
        :dose_unit_source_value),
    FunSQL.SQLTable(
        :procedure_occurrence,
        :procedure_occurrence_id,
        :person_id,
        :procedure_concept_id,
        :procedure_date,
        :procedure_datetime,
        :procedure_end_date,
        :procedure_end_datetime,
        :procedure_type_concept_id,
        :modifier_concept_id,
        :quantity,
        :provider_id,
        :visit_occurrence_id,
        :visit_detail_id,
        :procedure_source_value,
        :procedure_source_concept_id,
        :modifier_source_value),
    FunSQL.SQLTable(
        :device_exposure,
        :device_exposure_id,
        :person_id,
        :device_concept_id,
        :device_exposure_start_date,
        :device_exposure_start_datetime,
        :device_exposure_end_date,
        :device_exposure_end_datetime,
        :device_type_concept_id,
        :unique_device_id,
        :production_id,
        :quantity,
        :provider_id,
        :visit_occurrence_id,
        :visit_detail_id,
        :device_source_value,
        :device_source_concept_id,
        :unit_concept_id,
        :unit_source_value,
        :unit_source_concept_id),
    FunSQL.SQLTable(
        :measurement,
        :measurement_id,
        :person_id,
        :measurement_concept_id,
        :measurement_date,
        :measurement_datetime,
        :measurement_time,
        :measurement_type_concept_id,
        :operator_concept_id,
        :value_as_number,
        :value_as_concept_id,
        :unit_concept_id,
        :range_low,
        :range_high,
        :provider_id,
        :visit_occurrence_id,
        :visit_detail_id,
        :measurement_source_value,
        :measurement_source_concept_id,
        :unit_source_value,
        :unit_source_concept_id,
        :value_source_value,
        :measurement_event_id,
        :meas_event_field_concept_id),
    FunSQL.SQLTable(
        :observation,
        :observation_id,
        :person_id,
        :observation_concept_id,
        :observation_date,
        :observation_datetime,
        :observation_type_concept_id,
        :value_as_number,
        :value_as_string,
        :value_as_concept_id,
        :qualifier_concept_id,
        :unit_concept_id,
        :provider_id,
        :visit_occurrence_id,
        :visit_detail_id,
        :observation_source_value,
        :observation_source_concept_id,
        :unit_source_value,
        :qualifier_source_value,
        :value_source_value,
        :observation_event_id,
        :obs_event_field_concept_id),
    FunSQL.SQLTable(
        :death,
        :person_id,
        :death_date,
        :death_datetime,
        :death_type_concept_id,
        :cause_concept_id,
        :cause_source_value,
        :cause_source_concept_id),
    FunSQL.SQLTable(
        :note,
        :note_id,
        :person_id,
        :note_date,
        :note_datetime,
        :note_type_concept_id,
        :note_class_concept_id,
        :note_title,
        :note_text,
        :encoding_concept_id,
        :language_concept_id,
        :provider_id,
        :visit_occurrence_id,
        :visit_detail_id,
        :note_source_value,
        :note_event_id,
        :note_event_field_concept_id),
    FunSQL.SQLTable(
        :note_nlp,
        :note_nlp_id,
        :note_id,
        :section_concept_id,
        :snippet,
        :offset,
        :lexical_variant,
        :note_nlp_concept_id,
        :note_nlp_source_concept_id,
        :nlp_system,
        :nlp_date,
        :nlp_datetime,
        :term_exists,
        :term_temporal,
        :term_modifiers),
    FunSQL.SQLTable(
        :specimen,
        :specimen_id,
        :person_id,
        :specimen_concept_id,
        :specimen_type_concept_id,
        :specimen_date,
        :specimen_datetime,
        :quantity,
        :unit_concept_id,
        :anatomic_site_concept_id,
        :disease_status_concept_id,
        :specimen_source_id,
        :specimen_source_value,
        :unit_source_value,
        :anatomic_site_source_value,
        :disease_status_source_value),
    FunSQL.SQLTable(
        :fact_relationship,
        :domain_concept_id_1,
        :fact_id_1,
        :domain_concept_id_2,
        :fact_id_2,
        :relationship_concept_id),
    FunSQL.SQLTable(
        :location,
        :location_id,
        :address_1,
        :address_2,
        :city,
        :state,
        :zip,
        :county,
        :location_source_value,
        :country_concept_id,
        :country_source_value,
        :latitude,
        :longitude),
    FunSQL.SQLTable(
        :care_site,
        :care_site_id,
        :care_site_name,
        :place_of_service_concept_id,
        :location_id,
        :care_site_source_value,
        :place_of_service_source_value),
    FunSQL.SQLTable(
        :provider,
        :provider_id,
        :provider_name,
        :npi,
        :dea,
        :specialty_concept_id,
        :care_site_id,
        :year_of_birth,
        :gender_concept_id,
        :provider_source_value,
        :specialty_source_value,
        :specialty_source_concept_id,
        :gender_source_value,
        :gender_source_concept_id),
    FunSQL.SQLTable(
        :payer_plan_period,
        :payer_plan_period_id,
        :person_id,
        :payer_plan_period_start_date,
        :payer_plan_period_end_date,
        :payer_concept_id,
        :payer_source_value,
        :payer_source_concept_id,
        :plan_concept_id,
        :plan_source_value,
        :plan_source_concept_id,
        :sponsor_concept_id,
        :sponsor_source_value,
        :sponsor_source_concept_id,
        :family_source_value,
        :stop_reason_concept_id,
        :stop_reason_source_value,
        :stop_reason_source_concept_id),
    FunSQL.SQLTable(
        :cost,
        :cost_id,
        :cost_event_id,
        :cost_domain_id,
        :cost_type_concept_id,
        :currency_concept_id,
        :total_charge,
        :total_cost,
        :total_paid,
        :paid_by_payer,
        :paid_by_patient,
        :paid_patient_copay,
        :paid_patient_coinsurance,
        :paid_patient_deductible,
        :paid_by_primary,
        :paid_ingredient_cost,
        :paid_dispensing_fee,
        :payer_plan_period_id,
        :amount_allowed,
        :revenue_code_concept_id,
        :revenue_code_source_value,
        :drg_concept_id,
        :drg_source_value),
    FunSQL.SQLTable(
        :drug_era,
        :drug_era_id,
        :person_id,
        :drug_concept_id,
        :drug_era_start_date,
        :drug_era_end_date,
        :drug_exposure_count,
        :gap_days),
    FunSQL.SQLTable(
        :dose_era,
        :dose_era_id,
        :person_id,
        :drug_concept_id,
        :unit_concept_id,
        :dose_value,
        :dose_era_start_date,
        :dose_era_end_date),
    FunSQL.SQLTable(
        :condition_era,
        :condition_era_id,
        :person_id,
        :condition_concept_id,
        :condition_era_start_date,
        :condition_era_end_date,
        :condition_occurrence_count),
    FunSQL.SQLTable(
        :episode,
        :episode_id,
        :person_id,
        :episode_concept_id,
        :episode_start_date,
        :episode_start_datetime,
        :episode_end_date,
        :episode_end_datetime,
        :episode_parent_id,
        :episode_number,
        :episode_object_concept_id,
        :episode_type_concept_id,
        :episode_source_value,
        :episode_source_concept_id),
    FunSQL.SQLTable(
        :episode_event,
        :episode_id,
        :event_id,
        :episode_event_field_concept_id),
    FunSQL.SQLTable(
        :metadata,
        :metadata_id,
        :metadata_concept_id,
        :metadata_type_concept_id,
        :name,
        :value_as_string,
        :value_as_concept_id,
        :value_as_number,
        :metadata_date,
        :metadata_datetime),
    FunSQL.SQLTable(
        :cdm_source,
        :cdm_source_name,
        :cdm_source_abbreviation,
        :cdm_holder,
        :source_description,
        :source_documentation_reference,
        :cdm_etl_reference,
        :source_release_date,
        :cdm_release_date,
        :cdm_version,
        :cdm_version_concept_id,
        :vocabulary_version),
    FunSQL.SQLTable(
        :concept,
        :concept_id,
        :concept_name,
        :domain_id,
        :vocabulary_id,
        :concept_class_id,
        :standard_concept,
        :concept_code,
        :valid_start_date,
        :valid_end_date,
        :invalid_reason),
    FunSQL.SQLTable(
        :vocabulary,
        :vocabulary_id,
        :vocabulary_name,
        :vocabulary_reference,
        :vocabulary_version,
        :vocabulary_concept_id),
    FunSQL.SQLTable(
        :domain,
        :domain_id,
        :domain_name,
        :domain_concept_id),
    FunSQL.SQLTable(
        :concept_class,
        :concept_class_id,
        :concept_class_name,
        :concept_class_concept_id),
    FunSQL.SQLTable(
        :concept_relationship,
        :concept_id_1,
        :concept_id_2,
        :relationship_id,
        :valid_start_date,
        :valid_end_date,
        :invalid_reason),
    FunSQL.SQLTable(
        :relationship,
        :relationship_id,
        :relationship_name,
        :is_hierarchical,
        :defines_ancestry,
        :reverse_relationship_id,
        :relationship_concept_id),
    FunSQL.SQLTable(
        :concept_synonym,
        :concept_id,
        :concept_synonym_name,
        :language_concept_id),
    FunSQL.SQLTable(
        :concept_ancestor,
        :ancestor_concept_id,
        :descendant_concept_id,
        :min_levels_of_separation,
        :max_levels_of_separation),
    FunSQL.SQLTable(
        :source_to_concept_map,
        :source_code,
        :source_concept_id,
        :source_vocabulary_id,
        :source_code_description,
        :target_concept_id,
        :target_vocabulary_id,
        :valid_start_date,
        :valid_end_date,
        :invalid_reason),
    FunSQL.SQLTable(
        :drug_strength,
        :drug_concept_id,
        :ingredient_concept_id,
        :amount_value,
        :amount_unit_concept_id,
        :numerator_value,
        :numerator_unit_concept_id,
        :denominator_value,
        :denominator_unit_concept_id,
        :box_size,
        :valid_start_date,
        :valid_end_date,
        :invalid_reason),
    FunSQL.SQLTable(
        :concept_recommended,
        :concept_id_1,
        :concept_id_2,
        :relationship_id),
    FunSQL.SQLTable(
        :cohort,
        :cohort_definition_id,
        :subject_id,
        :cohort_start_date,
        :cohort_end_date),
    FunSQL.SQLTable(
        :cohort_definition,
        :cohort_definition_id,
        :cohort_definition_name,
        :cohort_definition_description,
        :definition_type_concept_id,
        :cohort_definition_syntax,
        :subject_concept_id,
        :cohort_initiation_date),
    dialect = :spark)

struct WriteCSVSpecification
    prefix::String
    node::FunSQL.SQLNode
    empty_cols::Vector{Symbol}
end

funsql_write_csv((prefix, node)::Pair{<:Union{Symbol, AbstractString}, <:Any};
                 empty_cols = Symbol[]) =
    WriteCSVSpecification(string(prefix), node, empty_cols)

function run(db, spec::WriteCSVSpecification)
    data = run(db, spec.node)
    dataframe = DataFrame(data)
    for col in spec.empty_cols
        insertcols!(dataframe, names(dataframe)[1], col => "")
    end
    when = Dates.format(Dates.now(),"yyyymmdd")
    filename = "$(spec.prefix)_$(when).csv"
    n_rows = size(dataframe)[1]
    CSV.write(filename, dataframe)
    @htl("""
        <div>$(dataframe)</div>
        <p>$n_rows rows written. Download <a href="$filename">$filename</a>.</p>
        <p><hr /></p>
    """)
end

function make_password()
    valid_characters = "ABCDEFGHJKLMNPQRSTUVWXYZabcdefghjkmnpqrstuvwzyz0123456789"
    return join(rand(valid_characters, 13))
end

function get_password()
    case = funsql_get_project_code()
    password = strip(get(ENV, "PASSWORD", ""))
    if length(password) == 0 && haskey(ENV, "CACHE_DIR")
        pwfile = joinpath(ENV["CACHE_DIR"], "password.txt")
        println(pwfile)
        if isfile(pwfile)
            password = strip(read(open(pwfile), String), "")
        else
            password = make_password()
            f = open(pwfile, "w")
            write(f, password * "\n")
            close(f)
        end
    end
    return password == "" ? nothing : password
end

struct WriteXLSXSpecification
    prefix::String
    node::FunSQL.SQLNode
end

""" @query write_encrypted_xlsx(prefix => content())

For this to work, include this boilerplate in your notebook:
```
    using JavaCall;
    JavaCall.isloaded() ? nothing : JavaCall.init()
    JavaCall.assertroottask_or_goodenv()
```
"""
funsql_write_encrypted_xlsx((prefix, node)::Pair{<:Union{Symbol, AbstractString}, <:Any}) =
    WriteXLSXSpecification(string(prefix), node)

function run(db, spec::WriteXLSXSpecification)
    @assert length(methods(TRDW.XLSX.write)) > 0 """To use write_encrypt you need:
      import JavaCall
      JavaCall.isloaded() ? nothing : JavaCall.init()
    """
    data = run(db, spec.node)
    dataframe = DataFrame(data)
    password = get_password()
    dataframe = DataFrame(data)
    n_rows = size(dataframe)[1]
    if isnothing(password)
        return @htl("<p>Password not available. Number of rows: $n_rows</p>")
    end
    when = Dates.format(Dates.now(),"yyyymmdd")
    filename = "$(spec.prefix)_$(when).xlsx"
    TRDW.XLSX.write(filename, dataframe; password)
    @htl("""
        <hr />
        <p>$n_rows rows written. Download <a href="$filename">$filename</a>.</p>
        <hr />
    """)
end
