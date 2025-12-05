module Prime


using ..TRDW: TRDW
export TRDW

using FunSQL


"""
    @funsql switch(test::Bool, q, [else_q])

Conditionally apply a combinator based on the value of the `test` parameter.

# Examples

```julia
@funsql valid_concept(; standard_only = true) = begin
    concept()
    filter(is_null(invalid_reason))
    switch(
        \$standard_only,
        filter(is_not_null(standard_concept)))
end
```
"""
@funsql switch(test::Bool, q, else_q = define()) =
    $(test ? q : else_q)

export funsql_switch

"""
    @funsql join_first(joinee, on; order_by, left = false)
    @funsql join_first(joinee; on, order_by, left = false)
    @funsql left_join_first(joinee, on; order_by)
    @funsql left_join_first(joinee; on, order_by)

Join each input row to the first matching row from `joinee`, where the order
of `joinee` rows is determined by the specified `order_by` list.  Unlike
a standard `join()`, `join_first()` guarantees that the output cardinality
does not increase.

# Examples

```julia
@funsql begin
    person()
    left_join_first(
        first_visit => visit(),
        person_id == first_visit.person_id,
        order_by = [first_visit.datetime])
end
```
"""
function join_first
end

@funsql join_first(joinee, on; order_by, left = false) =
    join_first(joinee; on = $on, order_by = $order_by, left = $left)

@funsql join_first(joinee; on, order_by, left = false) = begin
    partition(order_by = [missing], name = join_first)
    join($joinee, $on, left = $left)
    partition(join_first.row_number(), order_by = $order_by, name = join_first)
    filter(join_first.row_number() <= 1)
    undefine(join_first)
end

export funsql_join_first

@funsql left_join_first(joinee, on; order_by) =
    join_first($joinee, on = $on, order_by = $order_by, left = true)

@funsql left_join_first(joinee; on, order_by) =
    join_first($joinee, on = $on, order_by = $order_by, left = true)

export funsql_left_join_first

"""
    @funsql attach_first(joinee, on = true; by = [person_id], order_by)

Join each input row to the first matching row from `joinee` that has the same
`person_id` (or another key specified in `by`) and satisfies the optional
`on` predicate.  The order of `joinee` rows is determined by the specified
`order_by` list.  Unlike `join()`, `attach_first()` does not eliminate or
duplicate input rows.

# Examples

```julia
@funsql begin
    person()
    left_join_first(
        first_visit => visit(),
        person_id == first_visit.person_id,
        order_by = [first_visit.datetime])
end
"""
@funsql attach_first(joinee, on = true; name = $(FunSQL.label(convert(FunSQL.SQLNode, joinee))), by = [person_id], order_by) =
    left_join_first($joinee, on = $on && and(args = $[@funsql($key == $name.$key) for key in by]), order_by = $order_by)

export funsql_attach_first

"""
    @funsql attach_earliest(joinee, on = true; by = [person_id])

Join each input row to the earliest matching row from `joinee` (determined by
the `datetime` attribute) that has the same `person_id` (or another key
specified in `by`) and satisfies the optional `on` predicate.

# Examples

```julia
@funsql begin
    person()
    attach_earliest(first_visit => visit())
end
"""
@funsql attach_earliest(joinee, on = true; name = $(FunSQL.label(convert(FunSQL.SQLNode, joinee))), by = [person_id], order_by = [$name.datetime.asc(nulls = last)]) =
    attach_first($joinee, $on; by = $by, order_by = $order_by)

export funsql_attach_earliest

"""
    @funsql attach_latest(joinee, on = true; by = [person_id])

Join each input row to the latest matching row from `joinee` (determined by
the `datetime` attribute) that has the same `person_id` (or another key
specified in `by`) and satisfies the optional `on` predicate.

# Examples

```julia
@funsql begin
    person()
    attach_latest(latest_visit => visit())
end
"""
@funsql attach_latest(joinee, on = true; name = $(FunSQL.label(convert(FunSQL.SQLNode, joinee))), by = [person_id], order_by = [$name.datetime.desc(nulls = last)]) =
    attach_first($joinee, $on; by = $by, order_by = $order_by)

export funsql_attach_latest

"""
    @funsql concept()
    @funsql concept(concept_ids...)

Return records from the `CONCEPT` table:
- Without arguments, return all records;
- With one or more `concept_ids`, return records where `CONCEPT.CONCEPT_ID`
  matches the given values.

For clarity, some `CONCEPT` columns are omitted from the output.

# Examples

```julia
@funsql Male() =
    concept(8507)
```
"""
function funsql_concept
end

@funsql concept() = begin
    from(concept)
    as(omop)
    define(
        omop.concept_id,
        omop.concept_name,
        omop.vocabulary_id,
        omop.concept_code,
        omop.domain_id,
        omop.concept_class_id,
        omop.standard_concept,
        omop.invalid_reason)
end

@funsql concept(concept_ids...) = begin
    concept()
    filter(in(concept_id, $(concept_ids...)))
end

export funsql_concept


"""
    @funsql concept_like(code_or_name)
    @funsql concept_like(code, name)

Evaluate whether the input `CONCEPT` record matches the given patterns:
- With a single argument, returns `true` if either `CONCEPT_CODE` or `CONCEPT_NAME`
  matches the pattern.
- With two arguments, returns `true` if both `CONCEPT_CODE` and `CONCEPT_NAME`
  match their respective patterns.

Code matching is exact.  Name matching is case-insensitive, and the pattern may
include wildcards `%`, `…`, or `...`.

# Examples

```julia
@funsql Gender(code_or_name, name = nothing) = begin
    concept()
    filter(
        vocabulary_id == "Gender" &&
        concept_like(\$code_or_name, \$name))
end

@funsql Gender("F")

@funsql Gender("Female")

@funsql Gender("F", "Female")

@funsql Gender("Fem…")
```
"""
function funsql_concept_like(code_or_name, name = nothing)
    name_pattern = replace(something(name, code_or_name), "…" => "%", "..." => "%")
    if name === nothing
        @funsql concept_code == $code_or_name || ilike(concept_name, $name_pattern)
    else
        @funsql concept_code == $code_or_name && ilike(concept_name, $name_pattern)
    end
end

export funsql_concept_like


"""
    @funsql concept_descend()

Add descendant concepts to the input concept set.

This combinator should be used only with standard or custom concepts since
any non-standard concepts in the input will be excluded from the result.

# Examples

```julia
@funsql Hypertension() = begin
    concept(316866 #= Hypertensive disorder =#)
    concept_descend()
end
```
"""
@funsql concept_descend() = begin
    as(base)
    join(
        concept_ancestor => from(concept_ancestor),
        base.concept_id == concept_ancestor.ancestor_concept_id)
    join(
        concept(),
        concept_ancestor.descendant_concept_id == concept_id,
        optional = true)
    define(concept_id => concept_ancestor.descendant_concept_id)
end

export funsql_concept_descend


"""
    @funsql concept_relate(relationship_id)

For each input concept, emit related concepts as defined by the
specified `relationship_id`.

# Examples
```julia
@funsql begin
    concept()
    filter(vocabulary_id == "ICD10CM" && concept_code == "I10")
    concept_relate("Maps to")
end
```
"""
@funsql concept_relate(relationship_id) = begin
    as(base)
    join(
        concept_relationship => begin
            from(concept_relationship)
            filter(relationship_id == $relationship_id)
        end,
        base.concept_id == concept_relationship.concept_id_1)
    join(
        concept(),
        concept_relationship.concept_id_2 == concept_id,
        optional = true)
    define(concept_id => concept_relationship.concept_id_2)
end

export funsql_concept_relate


"""
    @funsql concept_include(concept_set)

Add the given concept set to the input concept set.  With no input, return
the given concept set.  Commonly used for defining composable vocabulary
combinators.

# Examples

```julia
@funsql SNOMED(code_or_name, name = nothing) =
    concept_include(
        begin
            concept()
            filter(
                vocabulary_id == "SNOMED" &&
                concept_like(\$code_or_name, \$name))
            concept_descend()
        end)

@funsql begin
    SNOMED("195967001", "Asthma")
    SNOMED("233604007", "Pneumonia")
end
```
"""
@funsql concept_include(concept_set) =
    append($concept_set)

export funsql_concept_include


"""
    @funsql concept_exclude(concept_set)

Remove the given concept set from the input concept set.

# Examples

```julia
@funsql begin
    SNOMED("73211009", "Diabetes mellitus")
    concept_exclude(SNOMED("11687002", "Gestational diabetes mellitus"))
end
```
"""
@funsql concept_exclude(concept_set) = begin
    left_join(concept_exclude => $concept_set.group(concept_id), concept_id == concept_exclude.concept_id)
    filter(is_null(concept_exclude.concept_id))
end

export funsql_concept_exclude


"""
    @funsql concept_intersect(concept_set)

Restrict the input concept set to concepts that also appear in the given concept
set.

# Examples

```julia
@funsql ChronicBackPain() = begin
    SNOMED("82423001", "Chronic pain")
    concept_intersect(SNOMED("161891005", "Backache"))
end
```
"""
@funsql concept_intersect(concept_set) =
    join(concept_intersect => $concept_set.group(concept_id), concept_id == concept_intersect.concept_id)

export funsql_concept_intersect


"""
    @funsql concept_deduplicate()

Remove duplicate concept records from the input concept set.
"""
@funsql concept_deduplicate() = begin
    partition(name = concept_deduplicate, by = [concept_id], order_by = [missing])
    filter(concept_deduplicate.row_number() <= 1)
end

export funsql_concept_deduplicate


"""
    @funsql care_site()
    @funsql care_site(concept_set)

Return records from the `CARE_SITE` table:
- Without arguments, return all records;
- With a given concept set, return all matching records.

For clarity, some `CARE_SITE` columns are renamed or omitted from the output.

# Examples

```julia
@funsql care_site(CareSite("Emergency Medicine"))
```
"""
function funsql_care_site
end

@funsql care_site() = begin
    from(care_site)
    as(omop)
    define(
        omop.care_site_id,
        omop.care_site_name,
        source_concept_id => omop.care_site_source_concept_id,
        specialty_source_concept_id => omop.specialty_source_concept_id)
end

@funsql care_site(concept_set) = begin
    care_site()
    filter(source_concept_id in $concept_set)
end

export funsql_care_site


"""
    @funsql CareSite(code_or_name; descend = true)
    @funsql CareSite(code, name; descend = true)

Generate a concept set for care sites.

Arguments should refer to a concept within care site concept hierarchy,
which includes locations (sites), departments, and department specialties.

# Example

```julia
@funsql TMC() =
    CareSite("Tufts Medical Center Parent")

@funsql EmergencyDepartment() =
    CareSite("Emergency Medicine")
```
"""
@funsql CareSite(code_or_name, name = nothing; descend = true) =
    concept_include(
        begin
            concept()
            filter(
                assert_valid_concept(
                    in(vocabulary_id, "clarity_loc", "clarity_dep", "zc_specialty_dep") &&
                        concept_like($code_or_name, $name),
                    $(:(CareSite($code_or_name, $name; descend = $descend)))))
            switch($descend, concept_descend())
        end)

export funsql_CareSite


"""
    @funsql provider()
    @funsql provider(concept_set)

Return records from the `PROVIDER` table:
- Without arguments, return all records;
- With a given concept set, return all matching records.

For clarity, some `PROVIDER` columns are renamed or omitted from the output.

# Examples

```julia
@funsql provider(Provider("Rheumatology"))
```
"""
function funsql_provider
end

@funsql provider() = begin
    from(provider)
    as(omop)
    define(
        omop.provider_id,
        omop.provider_name,
        omop.npi,
        omop.dea,
        concept_id => omop.specialty_concept_id,
        omop.care_site_id,
        omop.gender_concept_id,
        source_concept_id => omop.specialty_source_concept_id,
        omop.gender_source_concept_id)
end

@funsql provider(concept_set) = begin
    provider()
    filter(concept_id in $concept_set)
end

export funsql_provider


"""
    @funsql Provider(code_or_name; descend = true)
    @funsql Provider(code, name; descend = true)

Generate a concept set for provider's type or specialty.

Arguments should refer to a standard concept within the Provider domain, which
includes provider types such as Physician or Registered Nurse and specialties
such as Cardiology or Rheumatology.

# Example

```julia
@funsql Nurse() =
    Provider("Nurse")

@funsql Rheumatologist() =
    Provider("Rheumatology")
```
"""
@funsql Provider(code_or_name, name = nothing; descend = true) =
    concept_include(
        begin
            concept()
            filter(
                assert_valid_concept(
                    domain_id == "Provider" &&
                        standard_concept == "S" &&
                        concept_like($code_or_name, $name),
                    $(:(Provider($code_or_name, $name; descend = $descend)))))
            switch($descend, concept_descend())
        end)

export funsql_Provider


"""
    @funsql person()

Return records from the `PERSON` table.  For clarity, some columns are renamed
or omitted from the output.

# Examples

```julia
@funsql begin
    person()
    filter(gender_concept_id in Gender("MALE"))
    filter(birth_datetime >= "2000-01-01")
end
```
"""
@funsql person() = begin
    from(person)
    as(omop)
    define(
        omop.person_id,
        omop.tm_authorized,
        omop.pat_id,
        omop.mrn,
        omop.legacy_tmc_mrn,
        omop.ma_death_id,
        omop.mother_person_id,
        omop.father_person_id,
        omop.gender_concept_id,
        omop.birth_datetime,
        omop.death_datetime,
        omop.death_datetime_problem_concept_id,
        omop.race_concept_id,
        omop.ethnicity_concept_id,
        omop.provider_id,
        omop.gender_source_concept_id,
        omop.race_source_concept_id,
        omop.ethnicity_source_concept_id,
        omop.death_cause_source_concept_id)
end

export funsql_person


"""
    @funsql Gender()

Return all concepts representing patient sex.
"""
@funsql Gender() =
    concept_include(
        concept(
            8507 #= MALE =#,
            8532 #= FEMALE =#))

"""
    @funsql Gender(code, name)
    @funsql Gender(code_or_name)

Return a concept representing patient sex.

Arguments must refer to a concept from the standard OMOP *Gender* vocabulary.
"""
@funsql Gender(code_or_name, name = nothing) =
    concept_include(
        begin
            concept()
            filter(
                assert_valid_concept(
                    vocabulary_id == "Gender" && concept_like($code_or_name, $name),
                    $(:(Gender($code_or_name, $name)))))
        end)

export funsql_Gender


"""
    @funsql Race()

Return all concepts representing patient race per OMB standard.
"""
@funsql Race() =
    concept_include(
        concept(
            8515 #= Asian =#,
            8516 #= Black or African American =#,
            8527 #= White =#,
            8557 #= Native Hawaiian or Other Pacific Islander =#,
            8657 #= American Indian or Alaska Native =#,
            1546847 #= More than one race =#))

"""
    @funsql Race(code, name)
    @funsql Race(code_or_name)

Return a concept representing patient race.

Arguments must refer to a concept from the standard OMOP *Race* vocabulary.
"""
@funsql Race(code_or_name, name = nothing) =
    concept_include(
        begin
            concept()
            filter(
                assert_valid_concept(
                    vocabulary_id == "Race" && concept_like($code_or_name, $name),
                    $(:(Race($code_or_name, $name)))))
        end)

export funsql_Race


"""
    @funsql Ethnicity()

Return all concepts representing patient ethnicity per OMB standard.
"""
@funsql Ethnicity() =
    concept_include(
        concept(
            38003563 #= Hispanic or Latino =#,
            38003564 #= Not Hispanic or Latino =#))

"""
    @funsql Ethnicity(code, name)
    @funsql Ethnicity(code_or_name)

Return a concept representing patient ethnicity.

Arguments must refer to a concept from the standard OMOP *Ethnicity* vocabulary.
"""
@funsql Ethnicity(code_or_name, name = nothing) =
    concept_include(
        begin
            concept()
            filter(
                assert_valid_concept(
                    vocabulary_id == "Ethnicity" && concept_like($code_or_name, $name),
                    $(:(Ethnicity($code_or_name, $name)))))
        end)

export funsql_Ethnicity


"""
    @funsql observation_period()

Return records from the `OBSERVATION_PERIOD` table.  For clarity, some columns
are renamed or omitted from the output.

# Examples

```julia
@funsql begin
    person()
    attach_earliest(observation_period => observation_period())
end
```
"""
@funsql observation_period() = begin
    from(observation_period)
    as(omop)
    define(
        omop.observation_period_id,
        omop.person_id,
        datetime => omop.observation_period_start_date,
        end_datetime => omop.observation_period_end_date)
end

export funsql_observation_period


# Reexport those TRDW symbols that have not been overriden here.

_reexport() =
    for name in names(TRDW)
        !(name in names(@__MODULE__)) || continue
        @eval begin
            using ..TRDW: $name
            export $name
        end
    end

_reexport()

end
