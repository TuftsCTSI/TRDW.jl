const OMOP_CONCEPT_CASES = [
    "concept(concept_ids...), one id" =>
        () -> @funsql(concept(8507)),
    "concept(concept_ids...), two ids" =>
        () -> @funsql(concept(8507, 8532)),
    "matches_concept(code_or_name)" =>
        () -> @funsql(concept().filter(vocabulary_id == "Gender" && matches_concept("Fem…"))),
    "matches_concept(code, name)" =>
        () -> @funsql(concept().filter(vocabulary_id == "Gender" && matches_concept("F", "Female"))),
    "equals_or_in(l, r)" =>
        () -> @funsql(concept().filter(equals_or_in(vocabulary_id, "SNOMED"))),
    "equals_or_in(l, r::AbstractVector)" =>
        () -> @funsql(concept().filter(equals_or_in(vocabulary_id, $(["RxNorm", "RxNorm Extension"])))),
    "equals_or_in(l, r::Missing)" =>
        () -> @funsql(concept().filter(equals_or_in(invalid_reason, $missing))),
    "equals_or_in(l, r::Nothing)" =>
        () -> @funsql(concept().filter(equals_or_in(invalid_reason, $nothing))),
    "switch(true, q)" =>
        () -> @funsql(concept().switch(true, filter(is_not_null(standard_concept)))),
    "switch(false, q)" =>
        () -> @funsql(concept().switch(false, filter(is_not_null(standard_concept)))),
    "switch(test, q, else_q)" =>
        () -> @funsql(concept().switch(false, filter(is_not_null(standard_concept)), filter(is_null(standard_concept)))),
    "include_descendant_concepts() (as documented)" =>
        () -> @funsql(concept(316866).include_descendant_concepts()),
    "include_prefix_descendant_concepts() (as documented)" =>
        () -> @funsql(concept(1569133).include_prefix_descendant_concepts()),
    "map_to_related_concepts(relationship_id) (as documented)" =>
        () -> @funsql(concept().filter(vocabulary_id == "ICD10CM" && concept_code == "I10").map_to_related_concepts("Maps to")),
    "map_to_standard_concepts(domain_id) (as documented)" =>
        () -> @funsql(concept().filter(vocabulary_id == "ICD10CM" && concept_code == "I10").map_to_standard_concepts("Condition")),
    "include_concepts(concept_set) (as documented)" =>
        () -> @funsql(begin
            SNOMED("195967001", "Asthma")
            SNOMED("233604007", "Pneumonia")
        end),
    "include_concepts(concept_set), with no input" =>
        () -> @funsql(include_concepts(concept(8507))),
    "exclude_concepts(concept_set) (as documented)" =>
        () -> @funsql(begin
            SNOMED("73211009", "Diabetes mellitus")
            exclude_concepts(SNOMED("11687002", "Gestational diabetes mellitus"))
        end),
    "intersect_concepts(concept_set) (as documented)" =>
        () -> @funsql(begin
            SNOMED("82423001", "Chronic pain")
            intersect_concepts(SNOMED("161891005", "Backache"))
        end),
    "concept_spec_to_predicate(name, spec, shared)" =>
        () -> @funsql(concept().filter(concept_spec_to_predicate(ICD10CM, "I10, I60-I69", vocabulary_id == "ICD10CM"))),
]

const OMOP_VOCABULARY_CASES = [
    "Concept(vocabulary_id, code_or_name; concept_class_id) (as documented)" =>
        () -> @funsql(Concept("NAACCR", "Grade", concept_class_id = "NAACCR Variable")),
    "Concept(vocabulary_ids, code, name; descend = false)" =>
        () -> @funsql(Concept($(["RxNorm", "RxNorm Extension"]), "161", "acetaminophen", descend = false)),
    "Provenance(code_or_name) (as documented)" =>
        () -> @funsql(Provenance("EHR")),
    "Provenance(code, name; descend = false)" =>
        () -> @funsql(Provenance("OMOP4976890", "EHR", descend = false)),
    "Care_Site(code_or_name) (as documented)" =>
        () -> @funsql(Care_Site("Tufts Medical Center Parent")),
    "Care_Site(code_or_name; descend = false)" =>
        () -> @funsql(Care_Site("Emergency Medicine", descend = false)),
    "Provider(code_or_name) (as documented)" =>
        () -> @funsql(Provider("Rheumatology")),
    "Provider(code_or_name; descend = false)" =>
        () -> @funsql(Provider("Nurse", descend = false)),
    "Gender(code_or_name) (as documented)" =>
        () -> @funsql(Gender("M")),
    "Gender(code, name)" =>
        () -> @funsql(Gender("F", "FEMALE")),
    "Race(code_or_name) (as documented)" =>
        () -> @funsql(Race("Asian")),
    "Ethnicity(code_or_name) (as documented)" =>
        () -> @funsql(Ethnicity("Hispanic or Latino")),
    "Visit(code_or_name) (as documented)" =>
        () -> @funsql(begin
            Visit("IP")
            Visit("ERIP")
        end),
    "SNOMED(code, name) (as documented)" =>
        () -> @funsql(SNOMED("59621000", "Essential hypertension")),
    "SNOMED(code_or_name; concept_class_id, descend = false)" =>
        () -> @funsql(SNOMED("Essential hypertension", concept_class_id = "Disorder", descend = false)),
    "ICD10CM(code, name) (as documented)" =>
        () -> @funsql(ICD10CM("I10", "Essential (primary) hypertension")),
    "ICD10CM(code_or_name; descend = false)" =>
        () -> @funsql(ICD10CM("I10", descend = false)),
    "ICD10CM(; spec) (as documented)" =>
        () -> @funsql(ICD10CM(spec = "I60-I69, G45, G46, H34.0")),
    "ICD10CM(; spec, descend = false)" =>
        () -> @funsql(ICD10CM(spec = "I10", descend = false)),
    "Condition_Status(code_or_name) (as documented)" =>
        () -> @funsql(Condition_Status("Primary discharge diagnosis")),
    "RxNorm(code_or_name) (as documented)" =>
        () -> @funsql(RxNorm("acetaminophen")),
    "ATC(code, name) (as documented)" =>
        () -> @funsql(ATC("A10BJ", "Glucagon-like peptide-1 (GLP-1) analogues")),
    "CVX(code_or_name) (as documented)" =>
        () -> @funsql(CVX("influenza virus vaccine, unspecified formulation")),
    "Route(code_or_name) (as documented)" =>
        () -> @funsql(Route("Intravenous")),
    "UCUM(code_or_name) (as documented)" =>
        () -> @funsql(UCUM("mg")),
    "CPT4(code, name) (as documented)" =>
        () -> @funsql(CPT4("1013012", "Electrocardiogram, routine ECG with at least 12 leads")),
    "CPT4(; spec) (as documented)" =>
        () -> @funsql(CPT4(spec = "11600-11646")),
    "CPT4(; spec, descend = false)" =>
        () -> @funsql(CPT4(spec = "17311, 11600-11646", descend = false)),
    "HCPCS(code, name) (as documented)" =>
        () -> @funsql(HCPCS("A4615", "Cannula, nasal")),
    "ICD10PCS(code, name) (as documented)" =>
        () -> @funsql(ICD10PCS("021", "Heart and Great Vessels, Bypass")),
    "LOINC(code_or_name) (as documented)" =>
        () -> @funsql(LOINC("Body mass index (BMI) [Ratio]")),
]

const OMOP_TABLE_CASES = [
    "care_site(concept_set) (as documented)" =>
        () -> @funsql(care_site(Care_Site("Emergency Medicine"))),
    "provider(concept_set) (as documented)" =>
        () -> @funsql(provider(Provider("Rheumatology"))),
    # 1234567890 fails the NPI check digit, so it's no real provider's NPI.
    "provider(name_or_npi::String)" =>
        () -> @funsql(provider("1234567890")),
    "visit(concept_set) (as documented)" =>
        () -> @funsql(visit(Visit("IP"))),
    "visit_detail(concept_set) (as documented)" =>
        () -> @funsql(visit_detail(Visit("Observation Room"))),
    "condition(concept_set), SNOMED (as documented)" =>
        () -> @funsql(condition(SNOMED("59621000", "Essential hypertension"))),
    "condition(concept_set), ICD10CM (as documented)" =>
        () -> @funsql(condition(ICD10CM("I10", "Essential (primary) hypertension"))),
    "drug(concept_set) (as documented)" =>
        () -> @funsql(drug(RxNorm("acetaminophen"))),
    "procedure(concept_set) (as documented)" =>
        () -> @funsql(procedure(CPT4("1013012", "Electrocardiogram, routine ECG with at least 12 leads"))),
    "device(concept_set) (as documented)" =>
        () -> @funsql(device(HCPCS("A4615", "Cannula, nasal"))),
    "measurement(concept_set) (as documented)" =>
        () -> @funsql(measurement(LOINC("8867-4", "Heart rate"))),
    "observation(concept_set), SNOMED (as documented)" =>
        () -> @funsql(observation(SNOMED("32911000", "Homeless"))),
    "observation(concept_set), ICD10CM (as documented)" =>
        () -> @funsql(observation(ICD10CM("Z00.0", "Encounter for general adult medical examination"))),
    "note(concept_set) (as documented)" =>
        () -> @funsql(note(LOINC("18842-5", "Discharge summary"))),
]

const OMOP_COMBINATOR_CASES = [
    "has(name) (as documented)" =>
        () -> @funsql(begin
            from(condition_occurrence)
            left_join(
                visit => from(visit_occurrence),
                visit.visit_occurrence_id == visit_occurrence_id)
            define(has_visit => has(visit))
        end),
    "left_join_first(joinee, on; order_by) (as documented)" =>
        () -> @funsql(begin
            person()
            left_join_first(
                first_visit => visit(),
                person_id == first_visit.person_id,
                order_by = [first_visit.datetime])
        end),
    "left_join_first(joinee; on, order_by)" =>
        () -> @funsql(person().left_join_first(
            first_visit => visit(),
            on = person_id == first_visit.person_id,
            order_by = [first_visit.datetime])),
    "join_first(joinee, on; order_by)" =>
        () -> @funsql(person().join_first(
            first_visit => visit(),
            person_id == first_visit.person_id,
            order_by = [first_visit.datetime])),
    "join_first(joinee; on, order_by, left, private)" =>
        () -> @funsql(person().join_first(
            first_visit => visit(),
            on = person_id == first_visit.person_id,
            order_by = [first_visit.datetime],
            left = true,
            private = true)),
    "attach_first(joinee, on; order_by) (as documented)" =>
        () -> @funsql(begin
            person()
            attach_first(
                first_visit => visit(),
                person_id == first_visit.person_id,
                order_by = [first_visit.datetime])
        end),
    "attach_first(joinee; by, order_by)" =>
        () -> @funsql(visit().attach_first(
            first_condition => condition(),
            by = [person_id, visit_occurrence_id],
            order_by = [first_condition.datetime])),
    "attach_earliest(joinee) (as documented)" =>
        () -> @funsql(person().attach_earliest(first_visit => visit())),
    "attach_earliest(joinee, on; private)" =>
        () -> @funsql(person().attach_earliest(
            first_visit => visit(),
            first_visit.datetime >= birth_datetime,
            private = true)),
    "attach_latest(joinee) (as documented)" =>
        () -> @funsql(person().attach_latest(latest_visit => visit())),
    "attach_nearest(joinee, on) (as documented)" =>
        () -> @funsql(begin
            visit()
            attach_nearest(
                bmi => measurement(LOINC("39156-5")),
                dateadd_day(-30, datetime) ≤ bmi.datetime ≤ dateadd_day(30, datetime))
        end),
    "attach_group(joinee) (as documented)" =>
        () -> @funsql(begin
            person()
            attach_group(visits => visit())
            define(n_visit => coalesce(visits.count(), 0))
        end),
    "attach_group(joinee; by)" =>
        () -> @funsql(visit().attach_group(
            conditions => condition(),
            by = [person_id, visit_occurrence_id])),
    "attach_all(joinee)" =>
        () -> @funsql(person().attach_all(visits => visit())),
    "attach_all(joinee, on; name)" =>
        () -> @funsql(person().attach_all(
            visits => visit(),
            visits.datetime >= birth_datetime,
            name = person_visit)),
    "filter_with(joinee) (as documented)" =>
        () -> @funsql(person().filter_with(condition(SNOMED("Essential hypertension")))),
    "filter_with(joinee, on)" =>
        () -> @funsql(person().filter_with(
            hypertension => condition(SNOMED("Essential hypertension")),
            hypertension.datetime >= birth_datetime)),
    "filter_without(joinee) (as documented)" =>
        () -> @funsql(person().filter_without(condition(SNOMED("Essential hypertension")))),
    "filter_without(joinee, on)" =>
        () -> @funsql(person().filter_without(
            hypertension => condition(SNOMED("Essential hypertension")),
            hypertension.datetime >= birth_datetime)),
    "take_first(; order_by) (as documented)" =>
        () -> @funsql(visit().take_first(order_by = [datetime])),
    "take_first(; by, order_by)" =>
        () -> @funsql(condition().take_first(by = [person_id, visit_occurrence_id], order_by = [datetime])),
    "take_earliest() (as documented)" =>
        () -> @funsql(visit().take_earliest()),
    "take_earliest(; by)" =>
        () -> @funsql(condition().take_earliest(by = [person_id, visit_occurrence_id])),
    "take_latest() (as documented)" =>
        () -> @funsql(visit().take_latest()),
    "take_latest(; by)" =>
        () -> @funsql(condition().take_latest(by = [person_id, visit_occurrence_id])),
    "define_era(datetime, end_datetime) (as documented)" =>
        () -> @funsql(begin
            drug(RxNorm("ampicillin"))
            filter(provenance_concept_id in Provenance("EHR administration record"))
            filter(route_concept_id in Route("Intravenous"))
            define_era(datetime, dateadd_day(1, end_datetime))
            group(person_id, era)
            define(datetime => min(datetime), end_datetime => max(end_datetime))
        end),
    "define_era(datetime, end_datetime; by, name, after, private)" =>
        () -> @funsql(condition().define_era(
            datetime,
            end_datetime,
            by = [person_id, concept_id],
            name = condition_era,
            after = person_id,
            private = true)),
]

@testset "omop.jl" begin
    @testset "concept sets" begin
        @testset "$name renders" for (name, build) in OMOP_CONCEPT_CASES
            @test with_test_env(() -> build_and_render(build, context = name)) !== nothing
        end
    end
    @testset "vocabularies" begin
        @testset "$name renders" for (name, build) in OMOP_VOCABULARY_CASES
            @test with_test_env(() -> build_and_render(build, context = name)) !== nothing
        end
    end
    @testset "tables" begin
        @testset "$name renders" for (name, build) in OMOP_TABLE_CASES
            @test with_test_env(() -> build_and_render(build, context = name)) !== nothing
        end
    end
    @testset "combinators" begin
        @testset "$name renders" for (name, build) in OMOP_COMBINATOR_CASES
            @test with_test_env(() -> build_and_render(build, context = name)) !== nothing
        end
    end

    @testset "concept_spec_to_predicate rejects an invalid range" begin
        @test_throws AssertionError TRDW.funsql_concept_spec_to_predicate(:ICD10CM, "I69-I60", true)
        @test_throws AssertionError TRDW.funsql_concept_spec_to_predicate(:ICD10CM, "I6-I69", true)
    end
end
