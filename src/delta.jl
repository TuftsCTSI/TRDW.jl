function funsql_delta_ctes(table, schema, reference_schema, columns)
    latest = "latest_$table"
    previous = "previous_$table"
    @funsql begin
        with(
            $latest => from($(FunSQL.SQLTable(qualifiers = [:ctsi, Symbol(schema)], name = Symbol(table), columns = columns))),
            $previous => from($(FunSQL.SQLTable(qualifiers = [:ctsi, Symbol(reference_schema)], name = Symbol(table), columns = columns))))
    end
end

@funsql begin

# >>> DELTA PERSON >>>
"""
@funsql delta_person()

# Examples

```julia
@funsql delta_person()
```
"""
delta_person(schema="trdw_prime_latest", reference_schema="trdw_prime_previous") = begin
	from(`latest_person`)
	join(previous => from(`previous_person`), person_id == previous.person_id, left=true, right=true)
	select(latest_person_id => person_id, latest_pat_id => pat_id, previous_person_id => previous.person_id, previous_pat_id => previous.pat_id)
    delta_ctes("person", $schema, $reference_schema, $([:person_id, :pat_id]))
end

"""
@funsql person_records_changed()

# Examples

```julia
@funsql person_records_changed()
```
"""
person_records_changed(schema="trdw_prime_latest", reference_schema="trdw_prime_previous") = begin
	from(latest_person)
	join(previous => from(previous_person), person_id == previous.person_id)
	select(
        person_id,
		pat_id,
		pat_id_is_equal => pat_id == previous.pat_id || (isnull(pat_id) && isnull(previous.pat_id)) ? true : false,
		mother_person_id_is_equal => mother_person_id == previous.mother_person_id || (isnull(mother_person_id) && isnull(previous.mother_person_id)) ? true : false,
		father_person_id_is_equal => father_person_id == previous.father_person_id || (isnull(father_person_id) && isnull(previous.father_person_id)) ? true : false,
		gender_concept_id_is_equal => gender_concept_id == previous.gender_concept_id || (isnull(gender_concept_id) && isnull(previous.gender_concept_id)) ? true : false,
		birth_datetime_is_equal => birth_datetime == previous.birth_datetime || (isnull(birth_datetime) && isnull(previous.birth_datetime)) ? true : false,
		death_datetime_is_equal => death_datetime == previous.death_datetime || (isnull(death_datetime) && isnull(previous.death_datetime)) ? true : false,
		race_concept_id_is_equal => race_concept_id == previous.race_concept_id || (isnull(race_concept_id) && isnull(previous.race_concept_id)) ? true : false,
		ethnicity_concept_id_is_equal => ethnicity_concept_id == previous.ethnicity_concept_id || (isnull(ethnicity_concept_id) && isnull(previous.ethnicity_concept_id)) ? true : false,
		provider_id_is_equal => provider_id == previous.provider_id || (isnull(provider_id) && isnull(previous.provider_id)) ? true : false
    )
    define(
        record_unchanged => (
            pat_id_is_equal &&
            mother_person_id_is_equal && 
            father_person_id_is_equal &&
            gender_concept_id_is_equal &&
            birth_datetime_is_equal &&
            death_datetime_is_equal &&
            race_concept_id_is_equal &&
            ethnicity_concept_id_is_equal &&
            provider_id_is_equal
        ) ? true : false
    )
    filter(!record_unchanged)
    delta_ctes("person", $schema, $reference_schema, $([:person_id, :pat_id,
        :mother_person_id,
        :father_person_id,
        :gender_concept_id,
        :birth_datetime,
        :death_datetime,
        :race_concept_id,
        :ethnicity_concept_id,
        :provider_id
    ]))
end
            
"""
@funsql person_snapshot_delta()

# Examples

```julia
@funsql person_snapshot_delta()
```
"""
person_snapshot_delta(schema="trdw_prime_latest", reference_schema="trdw_prime_previous") = append(
    delta_person($schema, $reference_schema).filter(isnull(previous_person_id)).select(person_id => latest_person_id, pat_id => latest_pat_id, result => "added"),
    delta_person($schema, $reference_schema).filter(isnull(latest_person_id)).select(person_id => previous_person_id, pat_id => previous_pat_id, result => "disappeared"),
    person_records_changed($schema, $reference_schema).select(person_id, pat_id, result => "changed"),
)
# <<< DELTA PERSON <<<

# >>> DELTA VISIT_OCCURRENCE >>>
"""@funsql delta_visit_occurrence()
# Examples
```julia
@funsql delta_visit_occurrence()
```
"""
delta_visit_occurrence(schema="trdw_prime_latest", reference_schema="trdw_prime_previous") = begin
    from(`latest_visit_occurrence`)
    join(previous => from(`previous_visit_occurrence`), visit_occurrence_id == previous.visit_occurrence_id, left=true, right=true)
    select(latest_visit_occurrence_id => visit_occurrence_id, latest_pat_enc_csn_id => pat_enc_csn_id, previous_visit_occurrence_id => previous.visit_occurrence_id, previous_pat_enc_csn_id => previous.pat_enc_csn_id)
    delta_ctes("visit_occurrence", $schema, $reference_schema, $([:visit_occurrence_id, :pat_enc_csn_id]))
end
"""@funsql visit_occurrence_records_changed()
# Examples
```julia
@funsql visit_occurrence_records_changed()
```
"""
visit_occurrence_records_changed(schema="trdw_prime_latest", reference_schema="trdw_prime_previous") = begin
	from(latest_visit_occurrence)
	join(previous => from(previous_visit_occurrence), visit_occurrence_id == previous.visit_occurrence_id)
	select(
		visit_occurrence_id,
		pat_enc_csn_id,
		pat_enc_csn_id_is_equal => pat_enc_csn_id == previous.pat_enc_csn_id || (isnull(pat_enc_csn_id) && isnull(previous.pat_enc_csn_id))  ? true : false,
		person_id_is_equal => person_id == previous.person_id || (isnull(person_id) && isnull(previous.person_id))  ? true : false,
		visit_concept_id_is_equal => visit_concept_id == previous.visit_concept_id || (isnull(visit_concept_id) && isnull(previous.visit_concept_id))  ? true : false,
		visit_start_datetime_is_equal => visit_start_datetime == previous.visit_start_datetime || (isnull(visit_start_datetime) && isnull(previous.visit_start_datetime))  ? true : false,
		visit_end_datetime_is_equal => visit_end_datetime == previous.visit_end_datetime || (isnull(visit_end_datetime) && isnull(previous.visit_end_datetime))  ? true : false,
		visit_type_concept_id_is_equal => visit_type_concept_id == previous.visit_type_concept_id || (isnull(visit_type_concept_id) && isnull(previous.visit_type_concept_id))  ? true : false,
		provider_id_is_equal => provider_id == previous.provider_id || (isnull(provider_id) && isnull(previous.provider_id))  ? true : false,
		care_site_id_is_equal => care_site_id == previous.care_site_id || (isnull(care_site_id) && isnull(previous.care_site_id))  ? true : false
	)
	define(
		record_unchanged => (
			person_id_is_equal &&
			visit_concept_id_is_equal &&
			visit_start_datetime_is_equal &&
			visit_end_datetime_is_equal &&
			visit_type_concept_id_is_equal &&
            provider_id_is_equal &&
			care_site_id_is_equal
		) ? true : false
	)
	filter(
		!record_unchanged
	)
    delta_ctes("visit_occurrence", $schema, $reference_schema, $([:visit_occurrence_id, :pat_enc_csn_id,
        :person_id,
        :visit_concept_id,
        :visit_start_datetime,
        :visit_end_datetime,
        :visit_type_concept_id,
        :provider_id,
        :care_site_id
    ]))
end

"""@funsql visit_occurrence_snapshot_delta()
# Examples
```julia
@funsql visit_occurrence_snapshot_delta()
```
"""
visit_occurrence_snapshot_delta(schema="trdw_prime_latest", reference_schema="trdw_prime_previous") = append(
	delta_visit_occurrence($schema, $reference_schema).filter(isnull(previous_visit_occurrence_id)).select(visit_occurrence_id => latest_visit_occurrence_id, pat_enc_csn_id => latest_pat_enc_csn_id, result => "added"),
	delta_visit_occurrence($schema, $reference_schema).filter(isnull(latest_visit_occurrence_id)).select(visit_occurrence_id => previous_visit_occurrence_id, pat_enc_csn_id => previous_pat_enc_csn_id, result => "disappeared"),
	visit_occurrence_records_changed($schema, $reference_schema).select(visit_occurrence_id, pat_enc_csn_id, result => "changed")
)

# <<< DELTA VISIT_OCCURRENCE <<<

# >>> DELTA PROVIDER >>>
"""@funsql delta_provider()
# Examples
```julia
@funsql delta_provider()
```
"""
delta_provider(schema="trdw_prime_latest", reference_schema="trdw_prime_previous") = begin
    from(`latest_provider`)
    join(previous => from(`previous_provider`), provider_id == previous.provider_id, left=true, right=true)
    select(latest_provider_id => provider_id, previous_provider_id => previous.provider_id)
    delta_ctes("provider", $schema, $reference_schema, $([:provider_id]))
end

"""@funsql provider_records_changed()
# Examples
```julia
@funsql provider_records_changed()
```
"""
provider_records_changed(schema="trdw_prime_latest", reference_schema="trdw_prime_previous") = begin
	from(latest_provider)
	join(previous => from(previous_provider), provider_id == previous.provider_id)
	select(
		provider_id,
		provider_name_is_equal => provider_name == previous.provider_name || (isnull(provider_name) && isnull(previous.provider_name))  ? true : false,
		npi_is_equal => npi == previous.npi || (isnull(npi) && isnull(previous.npi))  ? true : false,
		dea_is_equal => dea == previous.dea || (isnull(dea) && isnull(previous.dea))  ? true : false,
		specialty_concept_id_is_equal => specialty_concept_id == previous.specialty_concept_id || (isnull(specialty_concept_id) && isnull(previous.specialty_concept_id))  ? true : false,
		care_site_id_is_equal => care_site_id == previous.care_site_id || (isnull(care_site_id) && isnull(previous.care_site_id))  ? true : false,
		year_of_birth_is_equal => year_of_birth == previous.year_of_birth || (isnull(year_of_birth) && isnull(previous.year_of_birth))  ? true : false,
		gender_concept_id_is_equal => gender_concept_id == previous.gender_concept_id || (isnull(gender_concept_id) && isnull(previous.gender_concept_id))  ? true : false
	)
	define(
		record_unchanged => (
			provider_name_is_equal &&
			npi_is_equal &&
			dea_is_equal &&
			specialty_concept_id_is_equal &&
			care_site_id_is_equal &&
			year_of_birth_is_equal &&
			gender_concept_id_is_equal
		) ? true : false
	)
	filter(
		!record_unchanged
	)
    delta_ctes("provider", $schema, $reference_schema, $([:provider_id, :provider_name, :npi, :dea, :specialty_concept_id, :care_site_id, :year_of_birth, :gender_concept_id
    ]))
end

"""@funsql provider_snapshot_delta()
# Examples
```julia
@funsql provider_snapshot_delta()
```
"""
provider_snapshot_delta(schema="trdw_prime_latest", reference_schema="trdw_prime_previous") = append(
    delta_provider($schema, $reference_schema).filter(isnull(previous_provider_id)).select(provider_id => latest_provider_id, result => "added"),
    delta_provider($schema, $reference_schema).filter(isnull(latest_provider_id)).select(provider_id => previous_provider_id, result => "disappeared"),
    provider_records_changed($schema, $reference_schema).select(provider_id, result => "changed")
)

# <<< DELTA PROVIDER <<<

# >>> DELTA CARE_SITE >>>
"""@funsql delta_care_site()
# Examples
```julia
@funsql delta_care_site()
```
"""
delta_care_site($schema, $reference_schema) = begin
	from(`latest_care_site`)
	join(previous => from(`previous_care_site`), care_site_id == previous.care_site_id, left=true, right=true)
	select(latest_care_site_id => care_site_id, latest_care_site_source_concept_id => care_site_source_concept_id, previous_care_site_id => previous.care_site_id, previous_care_site_source_concept_id => previous.care_site_source_concept_id)
    delta_ctes("care_site", $schema, $reference_schema, $([:care_site_id, :care_site_source_concept_id]))
end

"""@funsql care_site_records_changed()
# Examples
```julia
@funsql care_site_records_changed()
```
"""
care_site_records_changed(schema="trdw_prime_latest", reference_schema="trdw_prime_previous") = begin
	from(latest_care_site)
	join(previous => from(previous_care_site), care_site_id == previous.care_site_id)
	select(
		care_site_id,
		care_site_name_is_equal => care_site_name == previous.care_site_name || (isnull(care_site_name) && isnull(previous.care_site_name))  ? true : false,
		care_site_source_concept_id_is_equal => care_site_source_concept_id == previous.care_site_source_concept_id || (isnull(care_site_source_concept_id) && isnull(previous.care_site_source_concept_id))  ? true : false,
		specialty_source_concept_id_is_equal => specialty_source_concept_id == previous.specialty_source_concept_id || (isnull(specialty_source_concept_id) && isnull(previous.specialty_source_concept_id))  ? true : false
	)
	define(
		record_unchanged => (
			care_site_name_is_equal &&
			care_site_source_concept_id_is_equal &&
			specialty_source_concept_id_is_equal
		) ? true : false
	)
	filter(
		!record_unchanged
	)
    delta_ctes("care_site", $schema, $reference_schema, $([:care_site_id, :care_site_name, :care_site_source_concept_id, :specialty_source_concept_id
    ]))
end

"""@funsql care_site_snapshot_delta()
# Examples
```julia
@funsql care_site_snapshot_delta()
```
"""
care_site_snapshot_delta(schema="trdw_prime_latest", reference_schema="trdw_prime_previous") = append(
	delta_care_site($schema, $reference_schema).filter(isnull(previous_care_site_id)).select(care_site_id => latest_care_site_id, care_site_source_concept_id => latest_care_site_source_concept_id, result => "added"),
	delta_care_site($schema, $reference_schema).filter(isnull(latest_care_site_id)).select(care_site_id => previous_care_site_id, care_site_source_concept_id => previous_care_site_source_concept_id, result => "disappeared"),
	care_site_records_changed($schema, $reference_schema).select(care_site_id, result => "changed")
)

# <<< DELTA CARE_SITE <<<

# >>> DELTA CONDITION_OCCURRENCE >>>
"""@funsql delta_condition_occurrence()
# Examples
```julia
@funsql delta_condition_occurrence()
```
"""
delta_condition_occurrence($schema, $reference_schema) = begin
	from(`latest_condition_occurrence`)
	join(previous => from(`previous_condition_occurrence`), condition_occurrence_id == previous.condition_occurrence_id, left=true, right=true)
	select(latest_condition_occurrence_id => condition_occurrence_id, previous_condition_occurrence_id => previous.condition_occurrence_id)
    delta_ctes("condition_occurrence", $schema, $reference_schema, $([:condition_occurrence_id]))
end

"""@funsql condition_occurrence_records_changed()
# Examples
```julia
@funsql condition_occurrence_records_changed()
```
"""
condition_occurrence_records_changed(schema="trdw_prime_latest", reference_schema="trdw_prime_previous") = begin
	from(latest_condition_occurrence)
	join(previous => from(previous_condition_occurrence), condition_occurrence_id == previous.condition_occurrence_id)
	select(
		condition_occurrence_id,
		person_id_is_equal => person_id == previous.person_id || (isnull(person_id) && isnull(previous.person_id))  ? true : false,
		condition_concept_id_is_equal => condition_concept_id == previous.condition_concept_id || (isnull(condition_concept_id) && isnull(previous.condition_concept_id))  ? true : false,
		condition_start_datetime_is_equal => condition_start_datetime == previous.condition_start_datetime || (isnull(condition_start_datetime) && isnull(previous.condition_start_datetime))  ? true : false,
		condition_end_datetime_is_equal => condition_end_datetime == previous.condition_end_datetime || (isnull(condition_end_datetime) && isnull(previous.condition_end_datetime))  ? true : false,
		condition_type_concept_id_is_equal => condition_type_concept_id == previous.condition_type_concept_id || (isnull(condition_type_concept_id) && isnull(previous.condition_type_concept_id))  ? true : false,
		condition_status_concept_id_is_equal => condition_status_concept_id == previous.condition_status_concept_id || (isnull(condition_status_concept_id) && isnull(previous.condition_status_concept_id))  ? true : false,
		provider_id_is_equal => provider_id == previous.provider_id || (isnull(provider_id) && isnull(previous.provider_id))  ? true : false,
		visit_occurrence_id_is_equal => visit_occurrence_id == previous.visit_occurrence_id || (isnull(visit_occurrence_id) && isnull(previous.visit_occurrence_id))  ? true : false
	)
	define(
		record_unchanged => (
			person_id_is_equal &&
			condition_concept_id_is_equal &&
			condition_start_datetime_is_equal &&
			condition_end_datetime_is_equal &&
			condition_type_concept_id_is_equal &&
			condition_status_concept_id_is_equal &&
			provider_id_is_equal &&
			visit_occurrence_id_is_equal
		) ? true : false
	)
	filter(
		!record_unchanged
	)
    delta_ctes("condition_occurrence", $schema, $reference_schema, $([:condition_occurrence_id, :person_id, :condition_concept_id, :condition_start_datetime, :condition_end_datetime, :condition_type_concept_id, :condition_status_concept_id, :provider_id, :visit_occurrence_id
    ]))
end

"""@funsql condition_occurrence_snapshot_delta()
# Examples
```julia
@funsql condition_occurrence_snapshot_delta()
```
"""
condition_occurrence_snapshot_delta(schema="trdw_prime_latest", reference_schema="trdw_prime_previous") = append(
	delta_condition_occurrence($schema, $reference_schema).filter(isnull(previous_condition_occurrence_id)).select(condition_occurrence_id => latest_condition_occurrence_id, result => "added"),
	delta_condition_occurrence($schema, $reference_schema).filter(isnull(latest_condition_occurrence_id)).select(condition_occurrence_id => previous_condition_occurrence_id, result => "disappeared"),
	condition_occurrence_records_changed($schema, $reference_schema).select(condition_occurrence_id, result => "changed")
)
# <<< DELTA CONDITION_OCCURRENCE <<<

# >>> DELTA PROCEDURE_OCCURRENCE >>>
# TODO
# <<< DELTA PROCEDURE_OCCURRENCE <<<

# >>> DELTA DRUG_EXPOSURE >>>
# TODO
# <<< DELTA DRUG_EXPOSURE <<<

# >>> DELTA DEVICE_EXPOSURE >>>
# TODO
# <<< DELTA DEVICE_EXPOSURE <<<

# >>> DELTA OBSERVATION >>>
"""@funsql delta_observation()
# Examples
```julia
@funsql delta_observation()
```
"""
delta_observation($schema, $reference_schema) = begin
	from(`latest_observation`)
	join(previous => from(`previous_observation`), observation_id == previous.observation_id, left=true, right=true)
	select(latest_observation_id => observation_id, previous_observation_id => previous.observation_id)
    delta_ctes("observation", $schema, $reference_schema, $([:observation_id]))
end

"""@funsql observation_records_changed()
# Examples
```julia
@funsql observation_records_changed()
```
"""
observation_records_changed(schema="trdw_prime_latest", reference_schema="trdw_prime_previous") = begin
	from(latest_observation)
	join(previous => from(previous_observation), observation_id == previous.observation_id)
	select(
		observation_id,
		person_id_is_equal => person_id == previous.person_id || (isnull(person_id) && isnull(previous.person_id))  ? true : false,
		observation_concept_id_is_equal => observation_concept_id == previous.observation_concept_id || (isnull(observation_concept_id) && isnull(previous.observation_concept_id))  ? true : false,
		observation_datetime_is_equal => observation_datetime == previous.observation_datetime || (isnull(observation_datetime) && isnull(previous.observation_datetime))  ? true : false,
		observation_type_concept_id_is_equal => observation_type_concept_id == previous.observation_type_concept_id || (isnull(observation_type_concept_id) && isnull(previous.observation_type_concept_id))  ? true : false,
		value_as_number_is_equal => value_as_number == previous.value_as_number || (isnull(value_as_number) && isnull(previous.value_as_number))  ? true : false,
		value_as_string_is_equal => value_as_string == previous.value_as_string || (isnull(value_as_string) && isnull(previous.value_as_string))  ? true : false,
		value_as_concept_id_is_equal => value_as_concept_id == previous.value_as_concept_id || (isnull(value_as_concept_id) && isnull(previous.value_as_concept_id))  ? true : false,
		unit_concept_id_is_equal => unit_concept_id == previous.unit_concept_id || (isnull(unit_concept_id) && isnull(previous.unit_concept_id))  ? true : false,
		provider_id_is_equal => provider_id == previous.provider_id || (isnull(provider_id) && isnull(previous.provider_id))  ? true : false,
		visit_occurrence_id_is_equal => visit_occurrence_id == previous.visit_occurrence_id || (isnull(visit_occurrence_id) && isnull(previous.visit_occurrence_id))  ? true : false
	)
	define(
		record_unchanged => (
			person_id_is_equal &&
			observation_concept_id_is_equal &&
			observation_datetime_is_equal &&
			observation_type_concept_id_is_equal &&
			value_as_number_is_equal &&
			value_as_string_is_equal &&
			value_as_concept_id_is_equal &&
			unit_concept_id_is_equal &&
			provider_id_is_equal &&
			visit_occurrence_id_is_equal
		) ? true : false
	)
	filter(
		!record_unchanged
	)
    delta_ctes("observation", $schema, $reference_schema, $([:observation_id, :person_id, :observation_concept_id, :observation_datetime, :observation_type_concept_id, :value_as_number, :value_as_string, :value_as_concept_id, :unit_concept_id, :provider_id, :visit_occurrence_id
    ]))
end

"""@funsql observation_snapshot_delta()
# Examples
```julia
@funsql observation_snapshot_delta()
```
"""
observation_snapshot_delta(schema="trdw_prime_latest", reference_schema="trdw_prime_previous") = append(
	delta_observation($schema, $reference_schema).filter(isnull(previous_observation_id)).select(observation_id => latest_observation_id, result => "added"),
	delta_observation($schema, $reference_schema).filter(isnull(latest_observation_id)).select(observation_id => previous_observation_id, result => "disappeared"),
	observation_records_changed($schema, $reference_schema).select(observation_id, result => "changed")
)

# <<< DELTA OBSERVATION <<<

# >>> DELTA MEASUREMENT >>>
"""@funsql delta_measurement()
# Examples
```julia
@funsql delta_measurement()
```
"""
delta_measurement($schema, $reference_schema) = begin
	from(`latest_measurement`)
	join(previous => from(`previous_measurement`), measurement_id == previous.measurement_id, left=true, right=true)
	select(latest_measurement_id => measurement_id, previous_measurement_id => previous.measurement_id)
    delta_ctes("measurement", $schema, $reference_schema, $([:measurement_id]))
end

"""@funsql measurement_records_changed()
# Examples
```julia
@funsql measurement_records_changed()
```
"""
measurement_records_changed(schema="trdw_prime_latest", reference_schema="trdw_prime_previous") = begin
	from(latest_measurement)
	join(previous => from(previous_measurement), measurement_id == previous.measurement_id)
	select(
		measurement_id,
		person_id_is_equal => person_id == previous.person_id || (isnull(person_id) && isnull(previous.person_id))  ? true : false,
		measurement_concept_id_is_equal => measurement_concept_id == previous.measurement_concept_id || (isnull(measurement_concept_id) && isnull(previous.measurement_concept_id))  ? true : false,
		measurement_datetime_is_equal => measurement_datetime == previous.measurement_datetime || (isnull(measurement_datetime) && isnull(previous.measurement_datetime))  ? true : false,
		measurement_type_concept_id_is_equal => measurement_type_concept_id == previous.measurement_type_concept_id || (isnull(measurement_type_concept_id) && isnull(previous.measurement_type_concept_id))  ? true : false,
		operator_concept_id_is_equal => operator_concept_id == previous.operator_concept_id || (isnull(operator_concept_id) && isnull(previous.operator_concept_id))  ? true : false,
		value_as_number_is_equal => value_as_number == previous.value_as_number || (isnull(value_as_number) && isnull(previous.value_as_number))  ? true : false,
		value_as_concept_id_is_equal => value_as_concept_id == previous.value_as_concept_id || (isnull(value_as_concept_id) && isnull(previous.value_as_concept_id))  ? true : false,
		unit_concept_id_is_equal => unit_concept_id == previous.unit_concept_id || (isnull(unit_concept_id) && isnull(previous.unit_concept_id))  ? true : false,
		range_low_is_equal => range_low == previous.range_low || (isnull(range_low) && isnull(previous.range_low))  ? true : false,
		range_high_is_equal => range_high == previous.range_high || (isnull(range_high) && isnull(previous.range_high))  ? true : false,
		flag_concept_id_is_equal => flag_concept_id == previous.flag_concept_id || (isnull(flag_concept_id) && isnull(previous.flag_concept_id))  ? true : false,
		provider_id_is_equal => provider_id == previous.provider_id || (isnull(provider_id) && isnull(previous.provider_id))  ? true : false,
		visit_occurrence_id_is_equal => visit_occurrence_id == previous.visit_occurrence_id || (isnull(visit_occurrence_id) && isnull(previous.visit_occurrence_id))  ? true : false
	)
	define(
		record_unchanged => (
			person_id_is_equal &&
			measurement_concept_id_is_equal &&
			measurement_datetime_is_equal &&
			measurement_type_concept_id_is_equal &&
			operator_concept_id_is_equal &&
			value_as_number_is_equal &&
			value_as_concept_id_is_equal &&
			unit_concept_id_is_equal &&
			range_low_is_equal &&
			range_high_is_equal &&
			flag_concept_id_is_equal &&
			provider_id_is_equal &&
			visit_occurrence_id_is_equal
		) ? true : false
	)
	filter(
		!record_unchanged
	)
    delta_ctes("measurement", $schema, $reference_schema, $([:measurement_id, :person_id, :measurement_concept_id, :measurement_datetime, :measurement_type_concept_id, :operator_concept_id, :value_as_number, :value_as_concept_id, :unit_concept_id, :range_low, :range_high, :flag_concept_id, :provider_id, :visit_occurrence_id
    ]))
end

"""@funsql measurement_snapshot_delta()
# Examples
```julia
@funsql measurement_snapshot_delta()
```
"""
measurement_snapshot_delta(schema="trdw_prime_latest", reference_schema="trdw_prime_previous") = append(
	delta_measurement($schema, $reference_schema).filter(isnull(previous_measurement_id)).select(measurement_id => latest_measurement_id, result => "added"),
	delta_measurement($schema, $reference_schema).filter(isnull(latest_measurement_id)).select(measurement_id => previous_measurement_id, result => "disappeared"),
	measurement_records_changed($schema, $reference_schema).select(measurement_id, result => "changed")
)
# <<< DELTA MEASUREMENT <<<

# >>> DELTA OBSERVATION_PERIOD >>>
"""@funsql delta_observation_period()
# Examples
```julia
@funsql delta_observation_period()
```
"""
delta_observation_period($schema, $reference_schema) = begin
	from(`latest_observation_period`)
	join(previous => from(`previous_observation_period`), observation_period_id == previous.observation_period_id, left=true, right=true)
	select(latest_observation_period_id => observation_period_id, previous_observation_period_id => previous.observation_period_id)
    delta_ctes("observation_period", $schema, $reference_schema, $([:observation_period_id]))
end

"""@funsql observation_period_records_changed()
# Examples
```julia
@funsql observation_period_records_changed()
```
"""
observation_period_records_changed(schema="trdw_prime_latest", reference_schema="trdw_prime_previous") = begin
	from(latest_observation_period)
	join(previous => from(previous_observation_period), observation_period_id == previous.observation_period_id)
	select(
		observation_period_id,
		person_id_is_equal => person_id == previous.person_id || (isnull(person_id) && isnull(previous.person_id))  ? true : false,
		observation_period_start_date_is_equal => observation_period_start_date == previous.observation_period_start_date || (isnull(observation_period_start_date) && isnull(previous.observation_period_start_date))  ? true : false,
		observation_period_end_date_is_equal => observation_period_end_date == previous.observation_period_end_date || (isnull(observation_period_end_date) && isnull(previous.observation_period_end_date))  ? true : false,
		period_type_concept_id_is_equal => period_type_concept_id == previous.period_type_concept_id || (isnull(period_type_concept_id) && isnull(previous.period_type_concept_id))  ? true : false
	)
	define(
		record_unchanged => (
			person_id_is_equal &&
			observation_period_start_date_is_equal &&
			observation_period_end_date_is_equal &&
			period_type_concept_id_is_equal
		) ? true : false
	)
	filter(
		!record_unchanged
	)
    delta_ctes("observation_period", $schema, $reference_schema, $([:observation_period_id, :person_id, :observation_period_start_date, :observation_period_end_date, :period_type_concept_id
    ]))
end

"""@funsql measurement_snapshot_delta()
# Examples
```julia
@funsql measurement_snapshot_delta()
```
"""
observation_period_snapshot_delta(schema="trdw_prime_latest", reference_schema="trdw_prime_previous") = append(
	delta_observation_period($schema, $reference_schema).filter(isnull(previous_observation_period_id)).select(observation_period_id => latest_observation_period_id, result => "added"),
	delta_observation_period($schema, $reference_schema).filter(isnull(latest_observation_period_id)).select(observation_period_id => previous_observation_period_id, result => "disappeared"),
	observation_period_records_changed($schema, $reference_schema).select(observation_period_id, result => "changed")
)
# <<< DELTA OBSERVATION_PERIOD <<<
"""
@funsql snapshot_delta()

Create a summary table for all OMOP tables in any two TRDW-Prime snapshots.
    
    # Examples 

```julia
@funsql snapshot_delta()
```
"""
snapshot_delta(schema="trdw_prime_latest", reference_schema="trdw_prime_previous") = append(
	person_snapshot_delta($schema, $reference_schema).group(result).define(count(), table => "person"),
	visit_occurrence_snapshot_delta($schema, $reference_schema).group(result).define(count(), table => "visit_occurrence"),
	provider_snapshot_delta($schema, $reference_schema).group(result).define(count(), table => "provider"),
	care_site_snapshot_delta($schema, $reference_schema).group(result).define(count(), table => "care_site"),
	condition_occurrence_snapshot_delta($schema, $reference_schema).group(result).define(count(), table => "condition_occurrence"),
	# procedure_occurrence_snapshot_delta($schema, $reference_schema).group(result).define(count(), table => "procedure_occurrence"),
	# drug_exposure_snapshot_delta($schema, $reference_schema).group(result).define(count(), table => "drug_exposure"),
	# device_exposure_snapshot_delta($schema, $reference_schema).group(result).define(count(), table => "device_exposure"),
	observation_snapshot_delta($schema, $reference_schema).group(result).define(count(), table => "observation"),
	measurement_snapshot_delta($schema, $reference_schema).group(result).define(count(), table => "measurement"),
	observation_period_snapshot_delta($schema, $reference_schema).group(result).define(count(), table => "observation_period")
)


end