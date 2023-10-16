function temp_table!(etl, name, def)
    qualifiers = ("ctsi", "temp")
    ref = Ref{Pair{FunSQL.SQLTable, FunSQL.SQLClause}}()
    q = FunSQL.From(name) |> FunSQL.WithExternal(name => def, qualifiers = qualifiers,
                                                 handler = (p -> ref[] = p))
    FunSQL.render(etl.db, q)
    t, c = ref[]
    name_sql = FunSQL.render(etl.db,
                             FunSQL.ID("ctsi") |> FunSQL.ID("temp") |> FunSQL.ID(t.name))
    sql = FunSQL.render(etl.db, c)
    create_stmt = "CREATE TABLE $name_sql AS\n$sql"
    drop_stmt = "DROP TABLE $name_sql"
    push!(etl.create_stmts, create_stmt)
    push!(etl.drop_stmts, drop_stmt)
    return FunSQL.From(t)
end

function cleanup!(db)
    sql = """
        SELECT "DROP TABLE ctsi.temp." || table_name AS query
        FROM information_schema.tables
        WHERE table_catalog = 'ctsi'
          AND table_schema = 'temp'
          AND table_name like '%_2023%'
          AND table_name like '%z'
    """
    for query in collect(row.query for row in DBInterface.execute(db, sql))
        DBInterface.execute(db, query)
    end
end

function zipfile(filename, db, pairs...)
    z = ZipFile.Writer(filename)
    for (name, q) in pairs
        q !== nothing || continue
        f = ZipFile.addfile(z, name; method=ZipFile.Deflate)
        if q isa AbstractDataFrame
            CSV.write(f, q; bufsize = 2^23)
        else
            cr = DBInterface.execute(db, q)
            CSV.write(f, cr; bufsize = 2^23)
        end
    end
    close(z)
end

const NOOP = @funsql define()

abstract type OMOP_QuerySet end

struct OMOP_Transform <: OMOP_QuerySet
    condition_era::FunSQL.SQLNode
    condition_occurrence::FunSQL.SQLNode
    death::FunSQL.SQLNode
    device_exposure::FunSQL.SQLNode
    dose_era::FunSQL.SQLNode
    drug_era::FunSQL.SQLNode
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
    # filters for additional visits
    is_extra_visit_detail::FunSQL.SQLNode
    is_extra_visit_occurrence::FunSQL.SQLNode

    OMOP_Transform(;
        condition_era = NOOP,
        condition_occurrence = NOOP,
        death = NOOP,
        device_exposure = NOOP,
        dose_era = NOOP,
        drug_era = NOOP,
        drug_exposure = NOOP,
        measurement = NOOP,
        note = NOOP,
        note_nlp = NOOP,
        observation = NOOP,
        person = NOOP,
        procedure_occurrence = NOOP,
        specimen = NOOP,
        visit_detail = NOOP,
        visit_occurrence = NOOP,
        is_extra_visit_detail = NOOP,
        is_extra_visit_occurrence = NOOP) =
            new(condition_era,
                condition_occurrence,
                death,
                device_exposure,
                dose_era,
                drug_era,
                drug_exposure,
                measurement,
                note,
                note_nlp,
                observation,
                person,
                procedure_occurrence,
                specimen,
                visit_detail,
                visit_occurrence,
                is_extra_visit_detail,
                is_extra_visit_occurrence)
end

struct OMOP_Queries <: OMOP_QuerySet
    condition_era::FunSQL.SQLNode
    condition_occurrence::FunSQL.SQLNode
    death::FunSQL.SQLNode
    device_exposure::FunSQL.SQLNode
    dose_era::FunSQL.SQLNode
    drug_era::FunSQL.SQLNode
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
    # filters for additional visits
    is_extra_visit_detail::FunSQL.SQLNode
    is_extra_visit_occurrence::FunSQL.SQLNode

    OMOP_Queries(;
        condition_era = @funsql(from(condition_era)),
        condition_occurrence = @funsql(from(condition_occurrence)),
        death = @funsql(from(death)),
        device_exposure = @funsql(from(device_exposure)),
        dose_era = @funsql(from(dose_era)),
        drug_era = @funsql(from(drug_era)),
        drug_exposure = @funsql(from(drug_exposure)),
        measurement = @funsql(from(measurement)),
        note = @funsql(from(note)),
        note_nlp = @funsql(from(note_nlp)),
        observation = @funsql(from(observation)),
        person = @funsql(from(person)),
        procedure_occurrence = @funsql(from(procedure_occurrence)),
        specimen = @funsql(from(specimen)),
        visit_detail = @funsql(from(visit_detail)),
        visit_occurrence = @funsql(from(visit_occurrence)),
        is_extra_visit_detail = @funsql(true),
        is_extra_visit_occurrence = @funsql(true)) =
            new(condition_era,
                condition_occurrence,
                death,
                device_exposure,
                dose_era,
                drug_era,
                drug_exposure,
                measurement,
                note,
                note_nlp,
                observation,
                person,
                procedure_occurrence,
                specimen,
                visit_detail,
                visit_occurrence,
                is_extra_visit_detail,
                is_extra_visit_occurrence)
end


function (rhs::OMOP_Transform)(lhs::T)::T where {T<:OMOP_QuerySet}
    T(;
        condition_era = lhs.condition_era |> rhs.condition_era,
        condition_occurrence = lhs.condition_occurrence |> rhs.condition_occurrence,
        death = lhs.death |> rhs.death,
        device_exposure = lhs.device_exposure |> rhs.device_exposure,
        dose_era = lhs.dose_era |> rhs.dose_era,
        drug_era = lhs.drug_era |> rhs.drug_era,
        drug_exposure = lhs.drug_exposure |> rhs.drug_exposure,
        measurement = lhs.measurement |> rhs.measurement,
        note = lhs.note |> rhs.note,
        note_nlp = lhs.note_nlp |> rhs.note_nlp,
        observation = lhs.observation |> rhs.observation,
        person = lhs.person |> rhs.person,
        procedure_occurrence = lhs.procedure_occurrence |> rhs.procedure_occurrence,
        specimen = lhs.specimen |> rhs.specimen,
        visit_detail = lhs.visit_detail |> rhs.visit_detail,
        visit_occurrence = lhs.visit_occurrence |> rhs.visit_occurrence,
        is_extra_visit_detail = @funsql(
            $(lhs.is_extra_visit_detail) && $(rhs.is_extra_visit_detail)),
        is_extra_visit_occurrence = @funsql(
            $(lhs.is_extra_visit_occurrence) && $(rhs.is_extra_visit_occurrence)))
end

strip_hiv_events(base) =
    base |> OMOP_Transform(;
            condition_era = @funsql(filter_hiv_concepts(condition_concept_id)),
            condition_occurrence = @funsql(filter_hiv_concepts(condition_concept_id)),
            drug_era = @funsql(filter_hiv_concepts(drug_concept_id)),
            drug_exposure = @funsql(filter_hiv_concepts(drug_concept_id)),
            measurement = @funsql(filter_hiv_concepts(measurement_concept_id)),
            observation = @funsql(filter_hiv_concepts(observation_concept_id)),
            procedure_occurrence = @funsql(filter_hiv_concepts(procedure_concept_id)),
            specimen = @funsql(filter_hiv_concepts(specimen_concept_id)))

strip_person_dob(base) =
    base |> OMOP_Transform(;
        person = @funsql(begin
            define(month_of_birth => int(missing),
                   day_of_birth => int(missing),
                   birth_datetime => timestamp(missing))
        end))

strip_text_fields(base) =
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
        end))

strip_extra_visits(base) =
    base |> OMOP_Transform(;
            is_extra_visit_detail = @funsql(false),
            is_extra_visit_occurrence = @funsql(false))

function export_zip(filename, db, input_q::FunSQL.AbstractSQLNode;
                    queries::OMOP_Queries = OMOP_Queries(),
                    include_txt = false,
                    include_dob = false,
                    include_hiv = false,
                    include_mrn = false,
                    extra_visit = false)
    queries = include_txt ? queries : strip_text_fields(queries)
    queries = include_dob ? queries : strip_person_dob(queries)
    queries = include_hiv ? queries : strip_hiv_events(queries)
    queries = extra_visit ? queries : strip_extra_visits(queries)
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
            @funsql $(queries.person).restrict_by($cohort_q))
    observation_period_q =
        temp_table!(
            etl,
            "observation_period_$suffix",
            @funsql from(observation_period).restrict_by($person_q))
    visit_occurrence_q =
        temp_table!(
            etl,
            "visit_occurrence_x_$suffix",
            @funsql $(queries.visit_occurrence).restrict_by($person_q))
    visit_detail_q =
        temp_table!(
            etl,
            "visit_detail_x_$suffix",
            @funsql begin
                $(queries.visit_detail)
                restrict_by($person_q)
                restrict_by(visit_occurrence_id, $visit_occurrence_q)
            end)
    restrict_q = @funsql begin
        restrict_by($person_q)
        restrict_by(visit_occurrence_id, $visit_occurrence_q)
        restrict_by(visit_detail_id, $visit_detail_q)
    end
    condition_occurrence_q =
        temp_table!(
            etl,
            "condition_occurrence_$suffix",
            @funsql $(queries.condition_occurrence).$restrict_q)
    drug_exposure_q =
        temp_table!(
            etl,
            "drug_exposure_$suffix",
            @funsql $(queries.drug_exposure).$restrict_q)
    procedure_occurrence_q =
        temp_table!(
            etl,
            "procedure_occurrence_$suffix",
            @funsql $(queries.procedure_occurrence).$restrict_q)
    device_exposure_q =
        temp_table!(
            etl,
            "device_exposure_$suffix",
            @funsql $(queries.device_exposure).$restrict_q)
    measurement_q =
        temp_table!(
            etl,
            "measurement_$suffix",
            @funsql $(queries.measurement).$restrict_q)
    observation_q =
        temp_table!(
            etl,
            "observation_$suffix",
            @funsql $(queries.observation).$restrict_q)
    death_q =
        temp_table!(
            etl,
            "death_$suffix",
            @funsql $(queries.death).restrict_by($person_q))
    note_q =
        temp_table!(
            etl,
            "note_$suffix",
            @funsql $(queries.note).$restrict_q)
    note_nlp_q =
        temp_table!(
            etl,
            "note_nlp_$suffix",
            @funsql $(queries.note_nlp).restrict_by(note_id, $note_q))
    specimen_q =
        temp_table!(
            etl,
            "specimen_$suffix",
            @funsql $(queries.specimen).restrict_by($person_q))
    visit_detail_q =
            @funsql begin
                $visit_detail_q
                left_join(outer => begin
                    from(visit_detail)
                    restrict_by(
                        visit_detail_id,
                        append(
                            $condition_occurrence_q,
                            $drug_exposure_q,
                            $procedure_occurrence_q,
                            $device_exposure_q,
                            $measurement_q,
                            $observation_q,
                            $note_q))
                end, visit_detail_id == outer.visit_detail_id)
                filter($(queries.is_extra_visit_detail) ||
                       !isnull(outer.visit_detail_id))
            end
    visit_occurrence_q =
        temp_table!(
            etl,
            "visit_occurrence_$suffix",
            @funsql begin
                $visit_occurrence_q
                left_join(outer => begin
                    from(visit_occurrence)
                    restrict_by(
                        visit_occurrence_id,
                        append(
                            $condition_occurrence_q,
                            $drug_exposure_q,
                            $procedure_occurrence_q,
                            $device_exposure_q,
                            $measurement_q,
                            $observation_q,
                            $note_q,
                            $visit_detail_q))
                end, visit_occurrence_id == outer.visit_occurrence_id)
                filter($(queries.is_extra_visit_occurrence) ||
                       !isnull(outer.visit_occurrence_id))
            end)
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
            @funsql $(queries.drug_era).restrict_by($cohort_q))
    dose_era_q =
        temp_table!(
            etl,
            "dose_era_$suffix",
            @funsql $(queries.dose_era).restrict_by($cohort_q))
    condition_era_q =
        temp_table!(
            etl,
            "condition_era_$suffix",
            @funsql $(queries.condition_era).restrict_by($cohort_q))
    episode_q =
        temp_table!(
            etl,
            "episode_$suffix",
            @funsql from(episode).filter(false))
    episode_event_q =
        temp_table!(
            etl,
            "episode_event_$suffix",
            @funsql from(episode_event).filter(false))
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
    mrn_q = nothing
    if include_mrn
        mrn_q = """
        SELECT
          p.person_id,
          array_join(collect_set(gp.system_epic_mrn),';') epic_mrn,
          array_join(collect_set(gp.system_tuftssoarian_mrn),';') soarian_mrn
        FROM `temp`.`person_$suffix` p
        LEFT JOIN `person_map`.`person_map` pm ON p.person_id = pm.person_id
        LEFT JOIN (
          SELECT DISTINCT
            system_epic_id,
            system_epic_mrn,
            system_tuftssoarian_mrn
          FROM `main`.`global`.`patient`) AS gp
            ON pm.person_source_value = gp.system_epic_id
        GROUP BY p.person_id
        """
    end
    for stmt in etl.create_stmts
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
        "concept_ancestor.csv" => concept_ancestor_q,
        "keyfile.csv" => mrn_q)
    for stmt in etl.drop_stmts
        DBInterface.execute(db, stmt)
    end
end

function export_denormalized_zip(filename, db, input_q::FunSQL.AbstractSQLNode;
                                 queries::OMOP_Queries = OMOP_Queries(),
                                 utilize_dob = false, include_hiv = false)
    queries = include_hiv ? queries : strip_hiv_events(queries)
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
            @funsql $(queries.person).restrict_by($cohort_q))
    visit_occurrence_q =
        temp_table!(
            etl,
            "visit_occurrence_$suffix",
            @funsql $(queries.visit_occurrence).restrict_by($cohort_q))
    visit_detail_q =
        temp_table!(
            etl,
            "visit_detail_$suffix",
            @funsql begin
                $(queries.visit_detail)
                restrict_by($cohort_q)
                restrict_by(visit_occurrence_id, $(queries.visit_occurrence))
            end)
    restrict_q = @funsql begin
        restrict_by($cohort_q)
        restrict_by(visit_occurrence_id, $visit_occurrence_q)
        restrict_by(visit_detail_id, $visit_detail_q)
    end
    condition_occurrence_q = @funsql $(queries.condition_occurrence).$restrict_q
    drug_exposure_q = @funsql $(queries.drug_exposure).$restrict_q
    procedure_occurrence_q = @funsql $(queries.procedure_occurrence).$restrict_q
    device_exposure_q = @funsql $(queries.device_exposure).$restrict_q
    measurement_q = @funsql $(queries.measurement).$restrict_q
    observation_q = @funsql $(queries.observation).$restrict_q
    death_q = @funsql $(queries.death).restrict_by($cohort_q)
    note_q = @funsql $(queries.note).$restrict_q
    specimen_q = @funsql $(queries.specimen).restrict_by($cohort_q)
    age_q = @funsql datediff_year(person.birth_datetime, start_date)
    if !utilize_dob
        age_q = @funsql year(start_date) - person.year_of_birth
    end
    q = @funsql begin
        append(
            begin
                $visit_occurrence_q
                define(
                    event_type => "visit_occurrence",
                    event_id => visit_occurrence_id,
                    start_date => visit_start_date,
                    start_datetime => visit_start_datetime,
                    end_date => visit_end_date,
                    end_datetime => visit_end_datetime,
                    concept_id => visit_concept_id,
                    source_value => visit_source_value)
            end,
            begin
                $visit_detail_q
                define(
                    event_type => "visit_detail",
                    event_id => visit_detail_id,
                    start_date => visit_detail_start_date,
                    start_datetime => visit_detail_start_datetime,
                    end_date => visit_detail_end_date,
                    end_datetime => visit_detail_end_datetime,
                    concept_id => visit_detail_concept_id,
                    source_value => visit_detail_source_value)
            end,
            begin
                $condition_occurrence_q
                define(
                    event_type => "condition_occurrence",
                    event_id => condition_occurrence_id,
                    start_date => condition_start_date,
                    start_datetime => condition_start_datetime,
                    end_date => condition_end_date,
                    end_datetime => condition_end_datetime,
                    concept_id => condition_concept_id,
                    source_value => condition_source_value)
            end,
            begin
                $drug_exposure_q
                define(
                    event_type => "drug_exposure",
                    event_id => drug_exposure_id,
                    start_date => drug_exposure_start_date,
                    start_datetime => drug_exposure_start_datetime,
                    end_date => drug_exposure_end_date,
                    end_datetime => drug_exposure_end_datetime,
                    concept_id => drug_concept_id,
                    source_value => drug_source_value)
            end,
            begin
                $procedure_occurrence_q
                define(
                    event_type => "procedure_occurrence",
                    event_id => procedure_occurrence_id,
                    start_date => procedure_date,
                    start_datetime => procedure_datetime,
                    end_date => procedure_end_date,
                    end_datetime => procedure_end_datetime,
                    concept_id => procedure_concept_id,
                    source_value => procedure_source_value)
            end,
            begin
                $device_exposure_q
                define(
                    event_type => "device_exposure",
                    event_id => device_exposure_id,
                    start_date => device_exposure_start_date,
                    start_datetime => device_exposure_start_datetime,
                    end_date => device_exposure_end_date,
                    end_datetime => device_exposure_end_datetime,
                    concept_id => device_concept_id,
                    source_value => device_source_value)
            end,
            begin
                $measurement_q
                define(
                    event_type => "measurement",
                    event_id => measurement_id,
                    start_date => measurement_date,
                    start_datetime => measurement_datetime,
                    end_date => measurement_date,
                    end_datetime => measurement_datetime,
                    concept_id => measurement_concept_id,
                    source_value => measurement_source_value)
            end,
            begin
                $observation_q
                define(
                    event_type => "observation",
                    event_id => observation_id,
                    start_date => observation_date,
                    start_datetime => observation_datetime,
                    end_date => observation_date,
                    end_datetime => observation_datetime,
                    concept_id => observation_concept_id,
                    source_value => observation_source_value)
            end,
            begin
                $death_q
                define(
                    event_type => "death",
                    event_id => int(missing),
                    visit_occurrence_id => int(missing),
                    start_date => death_date,
                    start_datetime => death_datetime,
                    end_date => death_date,
                    end_datetime => death_datetime,
                    concept_id => cause_concept_id,
                    source_value => cause_source_value)
            end,
            begin
                $note_q
                define(
                    event_type => "note",
                    event_id => note_id,
                    start_date => note_date,
                    start_datetime => note_datetime,
                    end_date => note_date,
                    end_datetime => note_datetime,
                    concept_id => note_class_concept_id,
                    source_value => note_source_value)
            end,
            begin
                $specimen_q
                define(
                    event_type => "specimen",
                    event_id => specimen_id,
                    visit_occurrence_id => int(missing),
                    start_date => specimen_date,
                    start_datetime => specimen_datetime,
                    end_date => specimen_date,
                    end_datetime => specimen_datetime,
                    concept_id => specimen_concept_id,
                    source_value => specimen_source_value)
            end)
        join(person => $person_q, person_id == person.person_id)
        join(concept => from(concept), concept_id == concept.concept_id)
        order(person_id, start_datetime, event_type, event_id)
        select(
            person_id,
            event_type,
            event_id,
            visit_occurrence_id,
            starting => ifnull(start_datetime, start_date),
            ending => ifnull(end_datetime, end_date),
            age => case($age_q > 90, 90, $age_q),
            concept_id,
            concept.concept_name,
            source_value)
    end
    for stmt in etl.create_stmts
        DBInterface.execute(db, stmt)
    end
    df = DBInterface.execute(db, q) |> DataFrame
    ps = ["$(key.person_id).csv" => subdf
          for (key, subdf) in pairs(groupby(df, :person_id))]
    zipfile(
        filename,
        db,
        ps...)
    for stmt in etl.drop_stmts
        DBInterface.execute(db, stmt)
    end
end
