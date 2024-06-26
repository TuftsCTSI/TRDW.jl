abstract type OMOP_QuerySet end

NOOP = @funsql define()

struct OMOP_Transform <: OMOP_QuerySet
    condition_occurrence::FunSQL.SQLNode
    death::FunSQL.SQLNode
    device_exposure::FunSQL.SQLNode
    drug_exposure::FunSQL.SQLNode
    measurement::FunSQL.SQLNode
    note::FunSQL.SQLNode
    note_nlp::FunSQL.SQLNode
    observation::FunSQL.SQLNode
    person::FunSQL.SQLNode
    procedure_occurrence::FunSQL.SQLNode
    specimen::FunSQL.SQLNode
    visit_detail::FunSQL.SQLNode
    visit_occurrence::FunSQL.SQLNode

    OMOP_Transform(;
        condition_occurrence = NOOP,
        death = NOOP,
        device_exposure = NOOP,
        drug_exposure = NOOP,
        measurement = NOOP,
        note = NOOP,
        note_nlp = NOOP,
        observation = NOOP,
        person = NOOP,
        procedure_occurrence = NOOP,
        specimen = NOOP,
        visit_detail = NOOP,
        visit_occurrence = NOOP) =
            new(condition_occurrence,
                death,
                device_exposure,
                drug_exposure,
                measurement,
                note,
                note_nlp,
                observation,
                person,
                procedure_occurrence,
                specimen,
                visit_detail,
                visit_occurrence)
end

struct OMOP_Queries <: OMOP_QuerySet
    condition_occurrence::FunSQL.SQLNode
    death::FunSQL.SQLNode
    device_exposure::FunSQL.SQLNode
    drug_exposure::FunSQL.SQLNode
    measurement::FunSQL.SQLNode
    note::FunSQL.SQLNode
    note_nlp::FunSQL.SQLNode
    observation::FunSQL.SQLNode
    person::FunSQL.SQLNode
    procedure_occurrence::FunSQL.SQLNode
    specimen::FunSQL.SQLNode
    visit_detail::FunSQL.SQLNode
    visit_occurrence::FunSQL.SQLNode

    OMOP_Queries(;
        condition_occurrence = @funsql(from(condition_occurrence)),
        death = @funsql(from(death)),
        device_exposure = @funsql(from(device_exposure)),
        drug_exposure = @funsql(from(drug_exposure)),
        measurement = @funsql(from(measurement)),
        note = @funsql(from(note)),
        note_nlp = @funsql(from(note_nlp)),
        observation = @funsql(from(observation)),
        person = @funsql(from(person)),
        procedure_occurrence = @funsql(from(procedure_occurrence)),
        specimen = @funsql(from(specimen)),
        visit_detail = @funsql(from(visit_detail)),
        visit_occurrence = @funsql(from(visit_occurrence))) =
            new(condition_occurrence,
                death,
                device_exposure,
                drug_exposure,
                measurement,
                note,
                note_nlp,
                observation,
                person,
                procedure_occurrence,
                specimen,
                visit_detail,
                visit_occurrence)
end

OMOP_Queries(base::OMOP_Queries;
    condition_occurrence = nothing,
    death = nothing,
    device_exposure = nothing,
    drug_exposure = nothing,
    measurement = nothing,
    note = nothing,
    note_nlp = nothing,
    observation = nothing,
    person = nothing,
    procedure_occurrence = nothing,
    specimen = nothing,
    visit_detail = nothing,
    visit_occurrence = nothing) =
        OMOP_Queries(
            something(condition_occurrence, base.condition_occurrence),
            something(death, base.death),
            something(device_exposure, base.device_exposure),
            something(drug_exposure, base.drug_exposure),
            something(measurement, base.measurement),
            something(note, base.note),
            something(note_nlp, base.note_nlp),
            something(observation, base.observation),
            something(person, base.person),
            something(procedure_occurrence, base.procedure_occurrence),
            something(specimen, base.specimen),
            something(visit_detail, base.visit_detail),
            something(visit_occurrence, base.visit_occurrence),
            something(exta_visit_cohort, base.exta_visit_cohort))

function (rhs::OMOP_Transform)(lhs::T)::T where {T<:OMOP_QuerySet}
    T(;
        condition_occurrence = lhs.condition_occurrence |> rhs.condition_occurrence,
        death = lhs.death |> rhs.death,
        device_exposure = lhs.device_exposure |> rhs.device_exposure,
        drug_exposure = lhs.drug_exposure |> rhs.drug_exposure,
        measurement = lhs.measurement |> rhs.measurement,
        note = lhs.note |> rhs.note,
        note_nlp = lhs.note_nlp |> rhs.note_nlp,
        observation = lhs.observation |> rhs.observation,
        person = lhs.person |> rhs.person,
        procedure_occurrence = lhs.procedure_occurrence |> rhs.procedure_occurrence,
        specimen = lhs.specimen |> rhs.specimen,
        visit_detail = lhs.visit_detail |> rhs.visit_detail,
        visit_occurrence = lhs.visit_occurrence |> rhs.visit_occurrence)
end

redact_hiv_events(base) =
    base |> OMOP_Transform(;
            condition_occurrence = @funsql(filter_hiv_concepts(condition_concept_id)),
            drug_exposure = @funsql(filter_hiv_concepts(drug_concept_id)),
            measurement = @funsql(filter_hiv_concepts(measurement_concept_id)),
            observation = @funsql(filter_hiv_concepts(observation_concept_id)),
            procedure_occurrence = @funsql(filter_hiv_concepts(procedure_concept_id)),
            specimen = @funsql(filter_hiv_concepts(specimen_concept_id)))

redact_person_dob(base) =
    base |> OMOP_Transform(;
        person = @funsql(begin
            define(month_of_birth => int(missing),
                   day_of_birth => int(missing),
                   birth_datetime => timestamp(missing))
        end))

redact_text_fields(base) =
    base |> OMOP_Transform(;
        person = @funsql(begin
            define(location_id => int(missing),
                   person_source_value => string(missing))
        end),
        measurement = @funsql(begin
            define(value_source_value => string(missing))
        end),
        observation = @funsql(begin
            define(value_as_string => string(missing))
        end),
        note = @funsql(begin
            define(note_text => string(missing))
        end),
        note_nlp = @funsql(begin
            define(snippet => string(missing))
        end),
        visit_occurrence = @funsql(begin
            define(visit_source_value => string(missing),
                   discharged_to_source_value => string(missing),
                   admitted_from_source_value => string(missing))
        end))

function denormalize_concepts(base)
    trs = Pair{Symbol, FunSQL.SQLNode}[]
    for tbl_name in [:condition_occurrence,
                     :death,
                     :device_exposure,
                     :drug_exposure,
                     :measurement,
                     :note,
                     :note_nlp,
                     :observation,
                     :person,
                     :procedure_occurrence,
                     :specimen,
                     :visit_detail,
                     :visit_occurrence]
        tbl = omop_catalog[tbl_name]
        tr = @funsql as(base)
        for col_name in tbl.columns
            col_name_s = string(col_name)
            endswith(col_name_s, "_concept_id") || continue
            prefix = Symbol(col_name_s[1:end-11])
            tr = tr |> @funsql left_join($prefix => from(concept),
                                         base.$col_name == $prefix.concept_id)
        end
        for col_name in tbl.columns
            tr = tr |> @funsql define(base.$col_name)
            col_name_s = string(col_name)
            endswith(col_name_s, "_concept_id") || continue
            prefix = Symbol(col_name_s[1:end-11])
            vocabulary_id = Symbol("$(prefix)_vocabulary_id")
            concept_code = Symbol("$(prefix)_concept_code")
            concept_name = Symbol("$(prefix)_concept_name")
            tr = tr |> @funsql define(
                $vocabulary_id => $prefix.vocabulary_id,
                $concept_code => $prefix.concept_code,
                $concept_name => $prefix.concept_name)
        end
        push!(trs, tbl_name => tr)
    end
    base |> OMOP_Transform(; trs...)
end

struct QueryGuard
    qs::OMOP_Queries

    function QueryGuard(::Nothing; bypass::OMOP_Queries)
        new(bypass)
    end

    function QueryGuard(qs::OMOP_Queries = OMOP_Queries();
                        include_txt = false, include_dob = false,
                        include_hiv = false, denormalize = false)
        qs = include_txt ? qs : redact_text_fields(qs)
        qs = include_dob ? qs : redact_person_dob(qs)
        qs = include_hiv ? qs : redact_hiv_events(qs)
        qs = denormalize ? denormalize_concepts(qs) : qs
        new(qs)
    end

end

Base.getproperty(qg::QueryGuard, field::Symbol) =
    getproperty(getfield(qg, :qs), field)

struct ETLTiming
    name::String
    start::DateTime
    length::Float64
end

Base.show(io::IO, row::ETLTiming) =
    println(io, "$(row.start), $(row.length), $(row.name)")

Base.show(io::IO, timing::Vector{ETLTiming}) =
    for row in timing; print(io, row) end

struct ETLContext
    db::FunSQL.SQLConnection
    case::String
    cohort::Ref{FunSQL.SQLNode}
    queries::Ref{QueryGuard}
    create_stmts::Vector{String}
    drop_stmts::Vector{String}
    stmt_names::Vector{String}
    timing::Vector{ETLTiming}
    suffix::String

    function ETLContext(db::FunSQL.SQLConnection, case::String)
        new(db, case, Ref{FunSQL.SQLNode}(), Ref{QueryGuard}(),
            String[], String[], String[], ETLTiming[],
            Dates.format(Dates.now(), "yyyymmddHHMMSSZ"))
    end

end

function temp_table!(etl::ETLContext, name, def)
    qualifiers = ("ctsi", "temp")
    ref = Ref{Pair{FunSQL.SQLTable, FunSQL.SQLClause}}()
    q = FunSQL.From(name) |> FunSQL.WithExternal(name => def, qualifiers = qualifiers,
                                                 handler = (p -> ref[] = p))
    @debug name
    @debug sprint(FunSQL.pprint, q)
    @debug sprint(FunSQL.pprint, def)
    FunSQL.render(etl.db, q)
    t, c = ref[]
    name_sql = FunSQL.render(etl.db,
                   FunSQL.ID("ctsi") |> FunSQL.ID("temp") |> FunSQL.ID(t.name))
    sql = FunSQL.render(etl.db, def)
    create_stmt = "CREATE OR REPLACE TABLE $name_sql AS\n$sql"
    drop_stmt = "DROP TABLE IF EXISTS $name_sql"
    push!(etl.stmt_names, name)
    push!(etl.create_stmts, create_stmt)
    push!(etl.drop_stmts, drop_stmt)
    return FunSQL.From(t)
end

function create_temp_tables!(etl::ETLContext)
    while(!isempty(etl.create_stmts))
        stmt = popfirst!(etl.create_stmts)
        name = popfirst!(etl.stmt_names)
        start = time()
        start_time = now()
        try
            @debug name
            @debug stmt
            DBInterface.execute(etl.db, stmt)
            ODBC.clear!(etl.db.raw)
        catch e
            println(stmt)
            throw(e)
        finally
            timing = timing = ETLTiming(name, start_time, time() - start)
            push!(etl.timing, timing)
            @debug timing
        end
    end
end

function drop_temp_tables!(etl::ETLContext)
    @debug "dropping temp tables"
    while(!isempty(etl.drop_stmts))
        stmt = popfirst!(etl.drop_stmts)
        try
            DBInterface.execute(etl.db, stmt)
            ODBC.clear!(etl.db.raw)
        catch e
            println(stmt)
            throw(e)
        end
    end
end

function cleanup!(etl::ETLContext)
    sql = """
        SELECT "DROP TABLE ctsi.temp." || table_name AS query
        FROM information_schema.tables
        WHERE table_catalog = 'ctsi'
          AND table_schema = 'temp'
          AND table_name like '%_2024%'
          AND table_name like '%z'
    """
    for query in collect(row.query for row in DBInterface.execute(etl.db, sql))
        DBInterface.execute(etl.db, query)
    end
    ODBC.clear!(etl.db.raw)
end

function build_cohort!(etl::ETLContext, cohort_q::FunSQL.AbstractSQLNode)
    if !isassigned(etl.cohort)
        atexit(() -> drop_temp_tables!(etl))
        etl.cohort[] =
            temp_table!(
                etl,
                "cohort_$(etl.suffix)",
                @funsql $cohort_q.filter(is_not_null(person_id)).group(person_id))
        create_temp_tables!(etl)
    end
    return etl.cohort[]
end

function build_kernel!(etl::ETLContext, cohort_q::FunSQL.AbstractSQLNode,
                       queries::QueryGuard=QueryGuard(OMOP_Queries()))
    cohort_q = build_cohort!(etl, cohort_q)
    person_q =
        temp_table!(
            etl,
            "person_$(etl.suffix)",
            @funsql $(queries.person).restrict_by($cohort_q))
    visit_occurrence_q =
        temp_table!(
            etl,
            "visit_occurrence_$(etl.suffix)",
            @funsql $(queries.visit_occurrence).restrict_by($person_q))
    visit_detail_q =
        temp_table!(
            etl,
            "visit_detail_$(etl.suffix)",
            @funsql begin
                $(queries.visit_detail)
                restrict_by($person_q)
                restrict_by(visit_occurrence_id, $visit_occurrence_q)
            end)
    restrict_q = @funsql begin
        restrict_by($person_q)
        left_join(vo => $visit_occurrence_q, vo.visit_occurrence_id == visit_occurrence_id)
        left_join(vd => $visit_detail_q, vd.visit_detail_id == visit_detail_id)
        define(visit_occurrence_id => vo.visit_occurrence_id,
               visit_detail_id => vd.visit_detail_id)
    end
    condition_occurrence_q =
        temp_table!(
            etl,
            "condition_occurrence_$(etl.suffix)",
            @funsql $(queries.condition_occurrence).$restrict_q)
    death_q =
        temp_table!(
            etl,
            "death_$(etl.suffix)",
            @funsql $(queries.death).restrict_by($person_q))
    device_exposure_q =
        temp_table!(
            etl,
            "device_exposure_$(etl.suffix)",
            @funsql $(queries.device_exposure).$restrict_q)
    drug_exposure_q =
        temp_table!(
            etl,
            "drug_exposure_$(etl.suffix)",
            @funsql $(queries.drug_exposure).$restrict_q)
    measurement_q =
        temp_table!(
            etl,
            "measurement_$(etl.suffix)",
            @funsql $(queries.measurement).$restrict_q)
    note_q =
        temp_table!(
            etl,
            "note_$(etl.suffix)",
            @funsql $(queries.note).$restrict_q)
    note_nlp_q =
        temp_table!(
            etl,
            "note_nlp_$(etl.suffix)",
            @funsql $(queries.note_nlp).restrict_by(note_id, $note_q))
    observation_q =
        temp_table!(
            etl,
            "observation_$(etl.suffix)",
            @funsql $(queries.observation).$restrict_q)
    procedure_occurrence_q =
        temp_table!(
            etl,
            "procedure_occurrence_$(etl.suffix)",
            @funsql $(queries.procedure_occurrence).$restrict_q)
    specimen_q =
        temp_table!(
            etl,
            "specimen_$(etl.suffix)",
            @funsql $(queries.specimen).restrict_by($person_q))

    etl.queries[] =
      QueryGuard(nothing; bypass=
        OMOP_Queries(;
          condition_occurrence = condition_occurrence_q,
          death = death_q,
          device_exposure = device_exposure_q,
          drug_exposure = drug_exposure_q,
          measurement = measurement_q,
          note = note_q,
          note_nlp = note_nlp_q,
          observation = observation_q,
          person = person_q,
          procedure_occurrence = procedure_occurrence_q,
          specimen = specimen_q,
          visit_detail = visit_detail_q,
          visit_occurrence = visit_occurrence_q))
end

function zipfile(filename, db, pairs...)
    @assert endswith(filename, ".zip")
    folder = joinpath("/run/notebooks/cache", filename[1:end-4])
    try
        mkpath(folder)
        for (name, q) in pairs
            if q isa AbstractDataFrame
                @debug "writing", name, size(q)
                CSV.write(joinpath(folder, name), q)
            else
                @debug "execute", name, q
                cr = DBInterface.execute(db, q)
                @debug "writing", name
                CSV.write(joinpath(folder, name), cr)
                ODBC.clear!(db.raw)
            end
        end
        Base.run(`zip -q -j -r $(filename) $(folder)`)
    finally
        rm(folder; force=true, recursive=true)
    end
end

@funsql query_mrns(;include_dob=true) = begin
    as(cohort)
    join(from(person), cohort.person_id == person_id)
    left_join(g=>from(concept), g.concept_id == gender_concept_id)
    left_join(d=>from(death), d.person_id == person_id)
    define(sex => case(g.concept_id == 0, "", g.concept_code))
    define(birth => $(include_dob ?
                    @funsql(make_date(year_of_birth, month_of_birth, day_of_birth)) :
                    @funsql(year_of_birth)))
    define(death => $(include_dob ? @funsql(d.death_date) : @funsql(year(d.death_date))))
    left_join(epic => begin
        from(`global.patient`)
        group(system_epic_id)
        define(mrn => array_join(collect_set(system_epic_mrn), ";"))
    end, epic.system_epic_id == person_source_value)
    left_join(soarian =>
        from(`trdwlegacysoarian.omop_common_person_map`),
        soarian.person_id == person_id)
    with(
        `trdwlegacysoarian.omop_common_person_map` =>
            from($(FunSQL.SQLTable(qualifiers = [:ctsi, :trdwlegacysoarian],
                                   name = :omop_common_person_map,
                                   columns = [:person_id, :mrn]))),
        `global.patient` =>
            from($(FunSQL.SQLTable(qualifiers = [:main, :global],
                                   name = :patient,
                                   columns = [:id, :system_epic_id, :system_epic_mrn]))))
    select(person_id, sex, birth, death, mrn => epic.mrn, soarian_mrn => soarian.mrn)
end

function export_keyfile(filename, etl::ETLContext; include_dob=false)
    password = get_password()
    if length(password) < 1
        return
    end
    @debug "export_keyfile($(repr(filename)))"
    cohort_q = etl.cohort[]
    query = @funsql $cohort_q.query_mrns(;include_dob=$include_dob)
    query = @funsql $query.to_subject_id().order(subject_id)
    data = run(etl.db, query)
    @debug "writing", "mrn"
    write_xlsx(data, filename, password)
end

function export_zip(filename, etl::ETLContext)

    @debug "export_zip($(repr(filename)))"
    @assert isassigned(etl.queries)

    condition_occurrence_q = etl.queries[].condition_occurrence
    death_q = etl.queries[].death
    device_exposure_q = etl.queries[].device_exposure
    drug_exposure_q = etl.queries[].drug_exposure
    measurement_q = etl.queries[].measurement
    note_q = etl.queries[].note
    note_nlp_q = etl.queries[].note_nlp
    observation_q = etl.queries[].observation
    person_q = etl.queries[].person
    procedure_occurrence_q = etl.queries[].procedure_occurrence
    specimen_q = etl.queries[].specimen
    visit_detail_q = etl.queries[].visit_detail
    visit_occurrence_q = etl.queries[].visit_occurrence

    observation_period_q =
        temp_table!(
            etl,
            "observation_period_$(etl.suffix)",
            @funsql begin
                from(observation_period)
                restrict_by($person_q)
            end)
    provider_q =
        temp_table!(
            etl,
            "provider_$(etl.suffix)",
            @funsql begin
                from(provider)
                define(care_site_id=>int(missing),
                       year_of_birth=>int(missing),
                       provider_source_value=>string(missing))
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
            "care_site_$(etl.suffix)",
            @funsql begin
                from(care_site)
                define(care_site_source_value=>string(missing),
                       place_of_service_source_value=>string(missing))
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
            "location_$(etl.suffix)",
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
            "fact_$(etl.suffix)",
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
            "fact_relationship_$(etl.suffix)",
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
            "payer_plan_period_$(etl.suffix)",
            @funsql from(payer_plan_period).filter(false))
    cost_q =
        temp_table!(
            etl,
            "cost_$(etl.suffix)",
            @funsql from(cost).filter(false))
    episode_q =
        temp_table!(
            etl,
            "episode_$(etl.suffix)",
            @funsql from(episode).filter(false))
    episode_event_q =
        temp_table!(
            etl,
            "episode_event_$(etl.suffix)",
            @funsql from(episode_event).filter(false))
    metadata_q =
        temp_table!(
            etl,
            "metadata_$(etl.suffix)",
            @funsql from(metadata))
    cdm_source_q =
        temp_table!(
            etl,
            "cdm_source_$(etl.suffix)",
            @funsql from(cdm_source))
    vocabulary_q =
        temp_table!(
            etl,
            "vocabulary_$(etl.suffix)",
            @funsql from(vocabulary))
    domain_q =
        temp_table!(
            etl,
            "domain_$(etl.suffix)",
            @funsql from(domain))
    concept_class_q =
        temp_table!(
            etl,
            "concept_class_$(etl.suffix)",
            @funsql from(concept_class))
    relationship_q =
        temp_table!(
            etl,
            "relationship_$(etl.suffix)",
            @funsql from(relationship))
    drug_strength_q =
        temp_table!(
            etl,
            "drug_strength_$(etl.suffix)",
            @funsql from(drug_strength).restrict_by(drug_concept_id, $drug_exposure_q.group(drug_concept_id)))
    concept_q =
        temp_table!(
            etl,
            "concept_$(etl.suffix)",
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
            "concept_synonym_$(etl.suffix)",
            @funsql from(concept_synonym).restrict_by(concept_id, $concept_q))
    concept_relationship_q =
        temp_table!(
            etl,
            "concept_relationship_$(etl.suffix)",
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
            "concept_ancestor_$(etl.suffix)",
            @funsql begin
                from(concept_ancestor)
                join(
                    subset_1 => $concept_q,
                    ancestor_concept_id == subset_1.concept_id)
                join(
                    subset_2 => $concept_q,
                    descendant_concept_id == subset_2.concept_id)
            end)
    # TODO: rebuild these directly
    condition_era_q =
        temp_table!(
            etl,
            "condition_era_$(etl.suffix)",
            @funsql from(condition_era).filter(false))
    dose_era_q =
        temp_table!(
            etl,
            "dose_era_$(etl.suffix)",
            @funsql from(dose_era).filter(false))
    drug_era_q =
        temp_table!(
            etl,
            "drug_era_$(etl.suffix)",
            @funsql from(drug_era).filter(false))
    create_temp_tables!(etl)
    zipfile(
        filename,
        etl.db,
        "person.csv" =>
            @funsql($person_q.$postfix.order(person_id)),
        "observation_period.csv" =>
            @funsql($observation_period_q.$postfix.order(person_id, observation_period_start_date)),
        "visit_occurrence.csv" =>
            @funsql($visit_occurrence_q.$postfix.order(person_id, visit_start_date)),
        "visit_detail.csv" =>
            @funsql($visit_detail_q.$postfix.order(person_id, visit_detail_start_date)),
        "condition_occurrence.csv" =>
            @funsql($condition_occurrence_q.$postfix.order(person_id, condition_start_date)),
        "drug_exposure.csv" =>
            @funsql($drug_exposure_q.$postfix.order(person_id, drug_exposure_start_date)),
        "procedure_occurrence.csv" =>
            @funsql($procedure_occurrence_q.$postfix.order(person_id, procedure_date)),
        "device_exposure.csv" =>
            @funsql($device_exposure_q.$postfix.order(person_id, device_exposure_start_date)),
        "measurement.csv" =>
            @funsql($measurement_q.$postfix.order(person_id, measurement_date)),
        "observation.csv" =>
            @funsql($observation_q.$postfix.order(person_id, observation_date)),
        "death.csv" =>
            @funsql($death_q.$postfix.order(person_id)),
        "note.csv" =>
            @funsql($note_q.$postfix.order(person_id, note_date)),
        "note_nlp.csv" =>
            @funsql($note_nlp_q.order(note_id)),
        "specimen.csv" =>
            @funsql($specimen_q.$postfix.order(person_id, specimen_date)),
        "provider.csv" =>
            @funsql($provider_q.order(provider_id)),
        "care_site.csv" =>
            @funsql($care_site_q.order(care_site_id)),
        "location.csv" =>
            @funsql($location_q.order(location_id)),
        "fact_relationship.csv" =>
            @funsql($fact_relationship_q.$fact_postfix),
        #"payer_plan_period.csv" => @funsql($payer_plan_period_q.$postfix),
        #"cost.csv" => cost_q,
        #"drug_era.csv" => @funsql($drug_era_q.$postfix),
        #"dose_era.csv" => @funsql($dose_era_q.$postfix),
        #"condition_era.csv" => @funsql($condition_era_q.$postfix),
        #"episode.csv" => @funsql($episode_q.$postfix.order(person_id, episode_start_date)),
        #"episode_event.csv" => @funsql($episode_event_q.order(episode_id, event_id)),
        "metadata.csv" => metadata_q,
        "cdm_source.csv" => cdm_source_q,
        "vocabulary.csv" =>
            @funsql($vocabulary_q.order(vocabulary_id)),
        "domain.csv" =>
            @funsql($domain_q.order(domain_id)),
        "concept_class.csv" =>
            @funsql($concept_class_q.order(concept_class_id)),
        "relationship.csv" =>
            @funsql($relationship_q.order(relationship_id)),
        "drug_strength.csv" =>
            @funsql($drug_strength_q.order(drug_concept_id)),
        "concept.csv" =>
            @funsql($concept_q.order(concept_id)),
        "concept_synonym.csv" =>
            @funsql($concept_synonym_q.order(concept_id)),
        "concept_relationship.csv" =>
            @funsql($concept_relationship_q.order(concept_id_1, concept_id_2)),
        "concept_ancestor.csv" =>
            @funsql($concept_ancestor_q.order(ancestor_concept_id, descendant_concept_id))
        )
end

function export_timeline_zip(filename, etl::ETLContext)
    @debug "export_timeline_zip($(repr(filename)))"
    @assert isassigned(etl.queries)

    postfix = isnothing(etl.case) ?
        @funsql(define()) :
        @funsql to_subject_id($(etl.case); rename=false, assert=false)

    condition_occurrence_q = etl.queries[].condition_occurrence
    death_q = etl.queries[].death
    device_exposure_q = etl.queries[].device_exposure
    drug_exposure_q = etl.queries[].drug_exposure
    measurement_q = etl.queries[].measurement
    note_q = etl.queries[].note
    note_nlp_q = etl.queries[].note_nlp
    observation_q = etl.queries[].observation
    person_q = etl.queries[].person
    procedure_occurrence_q = etl.queries[].procedure_occurrence
    specimen_q = etl.queries[].specimen
    visit_detail_q = etl.queries[].visit_detail
    visit_occurrence_q = etl.queries[].visit_occurrence

    q = @funsql begin
        append(
            begin
                $death_q
                define(
                    event_sort => 0,
                    event_type => "death",
                    occurrence_id => int(missing),
                    visit_occurrence_id => int(missing),
                    start_date => death_date,
                    start_datetime => death_datetime,
                    end_date => date(missing),
                    end_datetime => timestamp(missing),
                    concept_id => cause_concept_id,
                    value_concept_id => int(missing),
                    source_value => cause_source_value)
            end,
            begin
                $visit_occurrence_q
                define(
                    event_sort => 1,
                    event_type => "visit_occurrence",
                    occurrence_id => visit_occurrence_id,
                    visit_occurrence_id => visit_occurrence_id,
                    start_date => visit_start_date,
                    start_datetime => visit_start_datetime,
                    end_date => visit_end_date,
                    end_datetime => visit_end_datetime,
                    concept_id => visit_concept_id,
                    value_concept_id => int(missing),
                    source_value => string(missing))
            end,
            begin
                $visit_detail_q
                define(
                    event_sort => 2,
                    event_type => "visit_detail",
                    occurrence_id => visit_detail_id,
                    visit_occurrence_id => visit_occurrence_id,
                    start_date => visit_detail_start_date,
                    start_datetime => visit_detail_start_datetime,
                    end_date => visit_detail_end_date,
                    end_datetime => visit_detail_end_datetime,
                    concept_id => visit_detail_concept_id,
                    value_concept_id => int(missing),
                    source_value => string(missing))
            end,
            begin
                $condition_occurrence_q
                define(
                    event_sort => 3,
                    event_type => "condition_occurrence",
                    occurrence_id => condition_occurrence_id,
                    visit_occurrence_id => visit_occurrence_id,
                    start_date => condition_start_date,
                    start_datetime => condition_start_datetime,
                    end_date => condition_end_date,
                    end_datetime => condition_end_datetime,
                    concept_id => condition_concept_id,
                    value_concept_id => condition_status_concept_id,
                    source_value =>  condition_source_value)
            end,
            begin
                $device_exposure_q
                define(
                    event_sort => 4,
                    event_type => "device_exposure",
                    occurrence_id => device_exposure_id,
                    visit_occurrence_id => visit_occurrence_id,
                    start_date => device_exposure_start_date,
                    start_datetime => device_exposure_start_datetime,
                    end_date => device_exposure_end_date,
                    end_datetime => device_exposure_end_datetime,
                    concept_id => device_concept_id,
                    value_concept_id => int(missing),
                    source_value => device_source_value)
            end,
            begin
                $drug_exposure_q
                define(
                    event_sort => 5,
                    event_type => "drug_exposure",
                    occurrence_id => drug_exposure_id,
                    visit_occurrence_id => visit_occurrence_id,
                    start_date => drug_exposure_start_date,
                    start_datetime => drug_exposure_start_datetime,
                    end_date => drug_exposure_end_date,
                    end_datetime => drug_exposure_end_datetime,
                    concept_id => drug_concept_id,
                    value_concept_id => route_concept_id,
                    source_value => drug_source_value)
            end,
            begin
                $measurement_q
                define(
                    event_sort => 6,
                    event_type => "measurement",
                    occurrence_id => measurement_id,
                    visit_occurrence_id => visit_occurrence_id,
                    start_date => measurement_date,
                    start_datetime => measurement_datetime,
                    end_date => date(missing),
                    end_datetime => timestamp(missing),
                    concept_id => measurement_concept_id,
                    value_concept_id => value_as_concept_id,
                    source_value => measurement_source_value)
            end,
            begin
                $note_q
                define(
                    event_sort => 7,
                    event_type => "note",
                    occurrence_id => note_id,
                    visit_occurrence_id => visit_occurrence_id,
                    start_date => note_date,
                    start_datetime => note_datetime,
                    end_date => date(missing),
                    end_datetime => timestamp(missing),
                    concept_id => note_class_concept_id,
                    value_concept_id => int(missing),
                    source_value => note_source_value)
            end,
            begin
                $observation_q
                define(
                    event_sort => 8,
                    event_type => "observation",
                    occurrence_id => observation_id,
                    visit_occurrence_id => visit_occurrence_id,
                    start_date => observation_date,
                    start_datetime => observation_datetime,
                    end_date => date(missing),
                    end_datetime => timestamp(missing),
                    concept_id => observation_concept_id,
                    value_concept_id => value_as_concept_id,
                    source_value => observation_source_value)
            end,
            begin
                $procedure_occurrence_q
                define(
                    event_sort => 9,
                    event_type => "procedure_occurrence",
                    occurrence_id => procedure_occurrence_id,
                    visit_occurrence_id => visit_occurrence_id,
                    start_date => procedure_date,
                    start_datetime => procedure_datetime,
                    end_date => date(missing),
                    end_datetime => timestamp(missing),
                    concept_id => procedure_concept_id,
                    value_concept_id => int(missing),
                    source_value => procedure_source_value)
            end,
            begin
                $specimen_q
                define(
                    event_sort => 10,
                    event_type => "specimen",
                    occurrence_id => specimen_id,
                    visit_occurrence_id => int(missing),
                    start_date => specimen_date,
                    start_datetime => specimen_datetime,
                    end_date => date(missing),
                    end_datetime => timestamp(missing),
                    concept_id => specimen_concept_id,
                    value_concept_id => anatomic_site_concept_id,
                    source_value => specimen_source_value)
            end)
        join(person => $person_q, person_id == person.person_id)
        join(concept => from(concept), concept_id == concept.concept_id)
        left_join(value => from(concept).filter(concept_id != 0),
                  value_concept_id == value.concept_id)
        define(datetime => coalesce(start_datetime, timestamp(start_date)))
        define(datetime_end => coalesce(end_datetime, timestamp(end_date)))
        $postfix
        order(person_id, datetime, event_sort, occurrence_id)
        select(
            person_id,
            event_type,
            occurrence_id,
            visit_occurrence_id,
            datetime,
            datetime_end,
            concept.concept_id,
            concept.vocabulary_id,
            concept.concept_code,
            concept.concept_name,
            value_concept_id => value.concept_id,
            value_vocabulary_id => value.vocabulary_id,
            value_concept_code => value.concept_code,
            value_concept_name => value.concept_name,
            source_value)
    end
    create_temp_tables!(etl)
    df = DBInterface.execute(etl.db, q) |> DataFrame
    ODBC.clear!(etl.db.raw)
    ps = ["$(getproperty(key, :person_id)).csv" => subdf
          for (key, subdf) in pairs(groupby(df, :person_id))]
    zipfile(
        filename,
        etl.db,
        ps...)
end

function unwrap_selection(mod::Module, tname::Symbol)
    parts = []
    for col in TRDW.omop_catalog[tname].columns
        push!(parts, col =>
              FunSQL.Get(col, over=FunSQL.Get(:omop, over = FunSQL.Get(:base))))
    end
    sname = Symbol(split(string(tname), "_")[1])
    query = getfield(mod, Symbol("funsql_$(sname)_selection"))()
    query |> FunSQL.As(:base) |> FunSQL.Define(parts...)
end

UnwrapQueries(mod::Module) =
  OMOP_Queries(;
        condition_occurrence = unwrap_selection(mod, :condition_occurrence),
        device_exposure = unwrap_selection(mod, :device_exposure),
        drug_exposure = unwrap_selection(mod, :drug_exposure),
        measurement = unwrap_selection(mod, :measurement),
        note = unwrap_selection(mod, :note),
        note_nlp = @funsql(from(note_nlp).filter(false)),
        observation = unwrap_selection(mod, :observation),
        person = @funsql(from(person)),
        procedure_occurrence = unwrap_selection(mod, :procedure_occurrence),
        specimen = @funsql(from(specimen).filter(false)),
        visit_detail = @funsql(from(visit_detail).filter(false)),
        visit_occurrence = unwrap_selection(mod, :visit_occurrence))
